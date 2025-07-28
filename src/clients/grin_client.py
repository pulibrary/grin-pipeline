# client.py

# This module implements a GrinClient class that can be used
# to access the Google Books library portal.  It borrows code
# from the example given in the GRIN Overview document (not
# publicly available.

import os
import httplib2
import httpx
import io
import csv
import functools
import time
import threading
from dotenv import load_dotenv

from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run_flow

load_dotenv()


# These are the authentication scopes you are requesting. These are the limited
# powers granted to the thing you give the token to. In this case, you're asking
# for a token that will give GRIN the permission to see your email address and
# profile information.
SCOPES = [
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
]

# This is the file you downloaded from console.developers.google.com when you
# created your 'project'. You need this to generate credentials. Once you've
# generated the credentials, you could delete this file.
sfile = os.getenv('GOOGLE_SECRETS_FILE')

# This file contains the authorization token ('access_token') shared with GRIN,
# and the refresh token ('refresh_token') used to issue access tokens when your
# current token has expired.
cfile = os.getenv('GOOGLE_TOKEN_FILE')


# How much we read/write when streaming response data.
OUTPUT_BLOCKSIZE = 1024 * 1024


class CredsMissingError(IOError):
    """Raised by CredentialsFactory() when credentials are missing."""


class GRINPermissionDeniedError(IOError):
    """GRIN says you're not allowed."""


class GoogleLoginError(IOError):
    """Something failed logging in to Google."""


def credentials_factory(credentials_file):
    """Use the oauth2 libraries to load our credentials."""
    storage = Storage(credentials_file)
    creds = storage.get()
    if creds is None:
        raise CredsMissingError()
    # If the credentials are expired, use the 'refresh_token' to generate a new one.
    if creds.access_token_expired:
        creds.refresh(httplib2.Http())
    return creds


def get_creds(credentials_file, secrets_file):
    # Get proper oauth2 credentials.
    try:
        creds = credentials_factory(credentials_file)
    except CredsMissingError:
        storage = Storage(credentials_file)
        creds = run_flow(flow_from_clientsecrets(secrets_file, scope=SCOPES), storage)
    return creds


# This is a wrapper/decorator to limit the rate of calls made to the
# GRIN API.  Can be used more generally.


def rate_limiter(max_calls, period):
    """
    Decorator to limit the rate of function calls by blocking until calls are allowed.

    :param max_calls: Maximum number of calls allowed in the period.
    :param period: Period in seconds over which max_calls are allowed.
    """

    def decorator(func):
        call_times = []
        lock = threading.Lock()

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal call_times
            while True:
                with lock:
                    now = time.time()
                    # Remove timestamps outside the period window
                    call_times = [t for t in call_times if now - t < period]

                    if len(call_times) < max_calls:
                        call_times.append(now)
                        break

                    # Calculate time to wait until next slot frees
                    earliest = min(call_times)
                    sleep_time = period - (now - earliest)
                    if sleep_time < 0:
                        sleep_time = 0.01  # minimal wait to yield
                time.sleep(sleep_time)
            return func(*args, **kwargs)

        return wrapper

    return decorator


def table_to_dictlist(table, fields) -> list:
    return [dict(zip(fields, row)) for row in table]


class GrinClient:
    def __init__(self, directory: str = "PRNC") -> None:
        self.base_url = "https://books.google.com/libraries"
        self.directory = directory
        self.creds = get_creds(cfile, sfile)
        self._converted = None
        self._all_books = None
        self._available = None
        self._in_process = None
        self._failed = None

    @property
    def auth_header(self):
        return {"Authorization": f"Bearer {self.creds.access_token}"}

    @rate_limiter(max_calls=30, period=60)
    def make_grin_request(self, url, method="GET", data=None):
        """Makes an HTTP request to grin using httpx
        and returns the response."""
        if method == "GET":
            response = httpx.get(url, headers=self.auth_header)
        elif method == "POST":
            response = httpx.post(url, headers=self.auth_header)
        else:
            raise ValueError(f"{method} method not supported by make_grin_request")

        return response

    def resource_url(self, resource_str):
        return f"{self.base_url}/{self.directory}/{resource_str}"

    def grin_data(self, book_type) -> list:
        url = self.resource_url(f"_{book_type}?format=text&mode=all")
        response = self.make_grin_request(url)
        tsv_data = io.StringIO(response.text)
        reader = csv.reader(tsv_data, delimiter="\t")
        table = []
        for row in reader:
            table.append(row)
        return table

    @property
    def failed_books(self):
        fields: list = [
            "barcode",
            "scanned_date",
            "processed_date",
            "analyzed_date",
            "convert_failed_date",
            "convert_failed_info",
            "ocrd_date",
            "detailed_conversion_info",
            "link",
        ]
        data = self.grin_data("failed")
        return table_to_dictlist(data, fields)

    @property
    def available_books(self):
        fields: list = [
            "barcode",
            "scanned_date",
            "processed_date",
            "analyzed_date",
            "ocrd_date",
            "link",
        ]
        data = self.grin_data("available")
        return table_to_dictlist(data, fields)

    @property
    def in_process_books(self):
        fields: list = [
            "barcode",
            "scanned_date",
            "processed_date",
            "analyzed_date",
            "ocrd_date",
            "link",
        ]
        data = self.grin_data("in_process")
        return table_to_dictlist(data, fields)

    @property
    def all_books(self):
        fields: list = [
            "barcode",
            "scanned_date",
            "processed_date",
            "analyzed_date",
            "converted_date",
            "ocrd_date",
            "link",
        ]
        data = self.grin_data("all_books")
        return table_to_dictlist(data, fields)

    @property
    def converted_books(self) -> list | None:
        """the GRIN for converted_books returns
        a different format from the other apis;
        the barcode needs to be extracted from the first field."""
        fields = ["file", "scanned_date", "converted_date", "downloaded_date", "link"]
        data = self.grin_data("converted")
        dictlist = []
        for row in data:
            barcode = row[0].split(".")[0]
            row_dict = dict(zip(fields, row))
            row_dict["barcode"] = barcode
            dictlist.append(row_dict)
        return dictlist

    def convert_book(self, barcode: str):
        url = f"{self.resource_url('_process')}?barcodes={barcode}"
        response = httpx.post(url=url, headers=self.auth_header, follow_redirects=True)
        return response

    def convert_books(self, barcode_list):
        responses = {}
        for barcode in barcode_list:
            url = f"{self.resource_url('_process')}?barcodes={barcode}"
            response = httpx.post(
                url=url, headers=self.auth_header, follow_redirects=True
            )

            with io.StringIO(response.text) as f:
                reader = csv.DictReader(f, delimiter="\t")
                for row in reader:
                    responses[row["Barcode"]] = row["Status"]

        return responses

    def convert(self, barcode_list: list):
        url = f"{self.resource_url('_process')}?process_format=json"
        data = {
            #  'barcodes': '\n'.join(barcode_list)
            "barcodes": barcode_list
        }
        breakpoint()
        response = httpx.post(
            url=url, headers=self.auth_header, follow_redirects=True, data=data
        )

        response_dict = {}
        with io.StringIO(response.text) as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                response_dict[row["Barcode"]] = row["Status"]

        return response_dict

    def download_file(self, url, outpath):
        with httpx.stream(
            "GET", url, headers=self.auth_header, follow_redirects=True
        ) as response:
            response.raise_for_status()
            with open(outpath, "wb") as f:
                for chunk in response.iter_bytes():
                    f.write(chunk)

    def download_book(self, barcode, target_dir):
        fname = f"{barcode}.tar.gz.gpg"
        src_url = self.resource_url(fname)
        outpath = f"{target_dir}/{fname}"
        self.download_file(src_url, outpath)
