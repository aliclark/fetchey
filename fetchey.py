#!/usr/bin/env python3.7

import sys
from multiprocessing import Process, Queue

import argparse
import csv
import gzip
import json
import logging
import requests
import xml
import xmltodict
import zipfile
from dataclasses import asdict, dataclass
from io import BytesIO, TextIOWrapper


# Next steps:
#
# 1. Tests
#
# 2. Argument validation
#
# 3. On initial request, if Content-Length is present then we can check if it is small enough to fit in memory, but
# if not then we should download directly to disk. In either case the code should remain abstract to this,
# and do automatic cleanup of files.
#
# 4. If we it's the case downloading a file fully before processing the first element results in unacceptable latency,
# or the API only has a streaming interface, then we can start to implement a streaming pipeline.
# This is more laborious - .zip contains its directory at the end (nb. the internet suggests each file has its own
# # header so bsdtar can therefore extract in a streaming fashion), and both XML and JSON are likely to have the
# elements contained within a single root object, which would require some navigating with a parser.
#
# 5. I'm sure all sorts of connection problems will come up.

def as_bool(x):
    if isinstance(x, str):
        return x.lower() in ['1', 'true', 'yes', 'y', 't']
    return x in [True, 1]


def as_float(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def do_nothing(*args, **kwargs):
    pass


class Registered:
    pass


class Container:
    def __init__(self, outer=None, inner=None):
        self.outer = outer
        self.inner = inner

    def open(self):
        return None

    def get_name(self):
        return self.__class__.__name__

    def get_full_name(self):
        name = self.get_name()
        if self.outer:
            outer = self
            while outer.outer:
                outer = outer.outer
                name = '{} > {}'.format(outer.get_name(), name)
        if self.inner:
            inner = self
            while inner.inner:
                inner = inner.inner
                name = '{} < {}'.format(name, inner.get_name())
        return name


class Schema(Container):
    def __init__(self, data=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data = data


class Opener(Container):
    def __init__(self, fd=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fd = fd

    @staticmethod
    def registered_openers(opener_type):
        def all_subclasses(cls):
            return set(cls.__subclasses__()).union(
                [s for c in cls.__subclasses__() for s in all_subclasses(c)])

        return [opener for opener in all_subclasses(opener_type) if issubclass(opener, Registered)]


class BinaryFileOpener(Opener):
    def open_binary(self, fd):
        fd.close = do_nothing
        for opener in Opener.registered_openers(BinaryFileOpener):
            fd.seek(0)
            logging.debug("{} ? {}".format(self.get_full_name(), opener.__name__))
            self.inner = opener(fd, outer=self)
            opened = self.inner.open()
            if not opened:
                self.inner = None
                continue
            logging.debug("{}".format(self.get_full_name()))
            return opened
        self.inner = None
        return None


class WebDownload(BinaryFileOpener):
    def __init__(self, url):
        super().__init__()
        self.url = url
        self.response = None

    def open(self):
        self.response = requests.get(self.url)
        return self.open_binary(BytesIO(self.response.content))

    def get_name(self):
        return '{}({})'.format(self.__class__.__name__, self.url)


class ZipSingle(Registered, BinaryFileOpener):
    def open(self):
        try:
            zf = zipfile.ZipFile(self.fd)
            if len(zf.namelist()) > 1:
                logging.warning("Received a ZIP with more than one file, ignoring the others")
            return self.open_binary(zf.open(zf.namelist()[0]))
        except zipfile.BadZipFile:
            return None


class Gzip(Registered, BinaryFileOpener):
    def open(self):
        try:
            return self.open_binary(BytesIO(gzip.GzipFile(fileobj=self.fd, mode='r').read()))
        except OSError:
            return None


class TextFileOpener(BinaryFileOpener):
    def open_text(self, fd):
        fd.close = do_nothing
        for opener in Opener.registered_openers(SchemaOpener):
            fd.seek(0)
            logging.debug("{} ? {}".format(self.get_full_name(), opener.__name__))
            self.inner = opener(fd, outer=self)
            opened = self.inner.open()
            if not opened:
                self.inner = None
                continue
            logging.debug("{}".format(self.get_full_name()))
            return opened
        self.inner = None
        return None


class Utf8(Registered, TextFileOpener):
    def open(self):
        try:
            return self.open_text(TextIOWrapper(self.fd, encoding='utf-8'))
        except (UnicodeError, UnicodeDecodeError):
            return None


class Utf16(Registered, TextFileOpener):
    def open(self):
        try:
            return self.open_text(TextIOWrapper(self.fd, encoding='utf-16'))
        except (UnicodeError, UnicodeDecodeError):
            return None


class SchemaOpener(Opener):
    def open_schema(self, data):
        if not data:
            return None
        for opener in Opener.registered_openers(Schema):
            logging.debug("{} ? {}".format(self.get_full_name(), opener.__name__))
            self.inner = opener(data, outer=self)
            opened = self.inner.open()
            if not opened:
                self.inner = None
                continue
            logging.debug("{}".format(self.get_full_name()))
            return opened
        self.inner = None
        return None


class Xml(Registered, SchemaOpener):
    def open(self):
        try:
            return self.open_schema(xmltodict.parse(self.fd.read()))
        except xml.parsers.expat.ExpatError:
            return None

    def get_ext_name(self):
        return 'xml'


class Json(Registered, SchemaOpener):
    def open(self):
        try:
            return self.open_schema(json.load(self.fd))
        except json.decoder.JSONDecodeError:
            return None

    def get_ext_name(self):
        return 'json'


class Csv(Registered, SchemaOpener):
    def open(self):
        try:
            dialect = csv.Sniffer().sniff(self.fd.read(1024))
            self.fd.seek(0)
            return self.open_schema([row for row in csv.DictReader(self.fd, dialect=dialect)])
        except csv.Error:
            return None

    def get_ext_name(self):
        return 'csv'


@dataclass
class Product:
    id: str
    name: str
    brand: str
    retailer: str
    price: float
    in_stock: bool
    source: str


class ProductsSchema(Schema):
    def source_name(self):
        return self.outer.get_ext_name()

    def products(self):
        for item in self.data:
            yield self.product(item)

    def product(self, item):
        # tidy keys
        item = dict([(key.strip().lower(), value) for key, value in item.items()])

        renames = {'latest_price': 'price', 'available': 'in_stock', 'instock': 'in_stock'}
        item = dict([(renames.get(key, key), value) for key, value in item.items()])

        return Product(item.get('id'), item.get('name'), item.get('brand'), item.get('retailer'),
                       as_float(item.get('price')), as_bool(item.get('in_stock')), self.source_name())


class PricesearcherProductsXml(Registered, ProductsSchema):
    def open(self):
        if not isinstance(self.outer, Xml):
            return None
        self.data = self.data.get('root')
        if not self.data:
            return None
        self.data = self.data.get('item')
        if not self.data:
            return None
        return self

    def products(self):
        for item in self.data:
            yield self.product(dict([(key, value.get('#text')) for (key, value) in item.items()
                                     if key != '@type']))


class PricesearcherProductsJson(Registered, ProductsSchema):
    def open(self):
        if not isinstance(self.outer, Json):
            return None
        if 'id' not in [key.strip().lower() for key in self.data[0].keys()]:
            return None
        return self


class PricesearcherProductsCsv(Registered, ProductsSchema):
    def open(self):
        if not isinstance(self.outer, Csv):
            return None
        if 'id' not in [key.strip().lower() for key in self.data[0].keys()]:
            return None
        return self


class ProductCsvSerializer:
    fieldnames = ['id', 'name', 'brand', 'retailer', 'price', 'in_stock', 'source']

    def __init__(self, fd):
        self.writer = csv.DictWriter(fd, fieldnames=ProductCsvSerializer.fieldnames)
        self.writer.writeheader()

    def output(self, product):
        self.writer.writerow(ProductCsvSerializer.to_dict(product))

    @staticmethod
    def to_dict(product):
        product_dict = asdict(product)
        row = dict([(field, product_dict[field]) for field in ProductCsvSerializer.fieldnames
                    if product_dict[field] is not None])
        if 'in_stock' in row:
            row['in_stock'] = 1 if row['in_stock'] else 0
        if 'price' in row:
            row['price'] = "{0:.2f}".format(row['price'])
        return row


def source_web_products(queue, url):
    data = WebDownload(url).open()
    if not isinstance(data, ProductsSchema):
        raise RuntimeError('Could not open products at URL: {}'.format(url))
    for product in data.products():
        queue.put(product)


def output_products_csv(queue, file_path):
    with open(file_path, 'w') as fd:
        serializer = ProductCsvSerializer(fd)
        while True:
            product = queue.get()
            if not product:
                return
            serializer.output(product)


def background(processor, *args):
    process = Process(target=processor, args=args)
    process.start()
    return process


def main():
    root = logging.getLogger()

    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(process)s [%(name)s] %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)

    parser = argparse.ArgumentParser()
    parser.add_argument('output', help="File path for CSV output")
    parser.add_argument('urls', nargs='+', help="HTTP(S) URLs to fetch products data")
    parser.add_argument('--verbose', '-v', action='store_true')
    args = parser.parse_args()

    if args.verbose:
        root.setLevel(logging.DEBUG)

    output_queue = Queue()
    source_processors = [background(source_web_products, output_queue, url) for url in args.urls]
    output_processor = background(output_products_csv, output_queue, args.output)
    for source_processor in source_processors:
        source_processor.join()
    output_queue.put(None)
    output_processor.join()


if __name__ == '__main__':
    main()
