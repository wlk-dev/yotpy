"""
Yotpy: An easy-to-use Python wrapper for the Yotpo web API.
"""

__version__ = "0.0.62"

import asyncio
import aiohttp

from json import loads as json_loads
from csv import DictWriter
from io import StringIO
from typing import AsyncGenerator, Iterator, Union, Optional, Callable, Union
from html import unescape
from urllib.parse import urlencode
from math import ceil
from itertools import chain
from .exceptions import CustomException, SessionNotCreatedError, FailedToGetTokenError, PreflightException, UploadException, SendException, UserNotFound, AppNotFound


class JSONTransformer:
    """
    A utility class for transforming JSON data into various formats.
    """

    @staticmethod
    def stream_json_data(json: dict, keys: tuple) -> Iterator[dict[str, Union[tuple, str]]]:
        """
        Stream the full key directory and its final value of a JSON object of arbitrary depth, including lists.

        This method recursively traverses a nested JSON object and yields dictionaries containing the full key
        directory (as tuples) and their associated values. It handles dictionaries and lists within the JSON object.

        Example:
            Input JSON:
            ```python
            json = \\
            { "a": 
                { "b":
                    [ { "c": "d" }, { "e": [1,2,3] } ]
                },
              "x": { "y": "z" }
            }
            # Streaming key directories and values:
            >>> print(list(JSONTransformer.stream_json_data(json, tuple())))
            >>> [{"keys": ("a", "b", "0", "c"), "value": "d"},
            >>>  {"keys": ("a", "b", "1", "e", "0"), "value": 1},
            >>>  {"keys": ("a", "b", "1", "e", "1"), "value": 2},
            >>>  {"keys": ("a", "b", "1", "e", "2"), "value": 3},
            >>>  {"keys": ("x", "y"), "value": "z"}]

            ```

        Args:
            json (dict): The JSON object to stream.
            keys (tuple): The current list of keys being traversed.

        Yields:
            dict[str, Union[tuple | str]]: A dictionary containing the current key "directory" (as a tuple) and its associated value.

        Returns:
            Iterator[dict[str, Union[tuple | str]]]: An iterator that yields dictionaries with the current key "directory" and its associated value.

        Note:
            Tuples have been used instead of lists for the keys argument, for performance at the cost of readability of this function.
        """

        for key, value in json.items():
            if isinstance(value, dict):
                # If the value is another dictionary, recursively call this method
                # with the updated keys "list".
                for result in JSONTransformer.stream_json_data(value, keys + (key,)):
                    yield result
            elif isinstance(value, list):
                # If the value is a list, iterate through the list and recursively
                # call this method with the updated keys "list" and index as an additional key.
                for index, item in enumerate(value):
                    if not isinstance(item, (dict, list)):
                        yield {'keys': keys + (key, str(index)), 'value': item}
                        continue

                    for result in JSONTransformer.stream_json_data(item, keys + (key, str(index))):
                        yield result
            else:
                # If the value is not a dictionary or list, yield a dictionary containing
                # the current key "list" (as a tuple) and value.
                yield {'keys': keys + (key,), 'value': value}

    @staticmethod
    def merge_on_key(key: str, list_1: list[dict], list_2: list[dict]) -> list[dict]:
        """
        Merges two lists of dictionaries into a single list, based on matching dictionary keys.

        Args:
            key (str): The key to match the dictionaries on.
            list_1 (list[dict]): The first list of dictionaries.
            list_2 (list[dict]): The second list of dictionaries.

        Returns:
            list[dict]: A merged list of dictionaries containing merged disctionaries based on matching keys from both input lists.
        """
        if not list_1 or not list_2:
            raise Exception(
                f"Cannot merge empty list(s).\nlist_1: length={len(list_1)}\nlist_2: length={len(list_2)}")

        # Use list comprehension to merge dictionaries that have matching keys
        return [
            dict(item_1, **item_2)
            for item_1 in list_1
            for item_2 in list_2
            if item_1.get(key, 0) == item_2.get(key, 1)
        ]

    @staticmethod
    def flatten(json: dict[str, any], sep: str, exclude: list[str] = [], include: list[str] = []) -> dict[str, any]:
        """
        Flatten a nested JSON object into a dictionary with flattened keys.

        This method takes a nested JSON object and converts it into a dictionary with
        keys that represent the nested structure using a specified separator. Optionally,
        you can provide a list of keys to exclude or include in the resulting dictionary.

        Example:
            Input JSON:
            ```python
            json = { "a": 
                { "b":
                    [ { "c": "d" }, { "e": [1,2,3] } ]
                },
                "x": { "y": "z" }
            }
            # Flattening with a "." separator:
            >>> print(JSONTransformer.flatten(json, "."))
            >>> {"a.b.0.c": "d",
            >>>  "a.b.1.e.0": 1,
            >>>  "a.b.1.e.1": 2,
            >>>  "a.b.1.e.2": 3,
            >>>  "x.y": "z"}
            ```
            Include and Exclude should be formatted with the complete nested key output in mind.

            e.g  `include=["a.b.0.c"]`, returns `{"a.b.0.c": "d"}` only.

        Args:
            json (dict): The JSON object to flatten.
            sep (str): The separator to use for joining nested keys.
            exclude (list[str]): If specified, exclude keys that match the specified keys and include all others.
            include (list[str]): If specified, only include keys that match the specified keys.

        Returns:
            dict[str, str]: A flattened dictionary with keys representing the nested structure.
        """

        # Stream the key-value pairs of the JSON object using the stream_json_data method.
        data = {}
        for item in JSONTransformer.stream_json_data(json, tuple()):
            keys = item['keys']
            key_str = sep.join(keys)
            if (exclude and key_str in exclude) or (include and key_str not in include):
                continue
            # Check if the key string matches the specified key strings for content and title.
            # If so, html.unescape the value, this is from Yotpo's side of things.
            # This is done so NLTK is able to properly process the reviews.
            if ("content", "title").count(key_str):
                data[key_str] = unescape(item['value'])
            else:
                data[key_str] = item['value']
        return data

    @staticmethod
    def flatten_list(json_list: list[dict], sep: str, exclude: list[str] = [], include: list[str] = []) -> list[dict[str, any]]:
        """
        Flattens a list of JSON objects into a list of dictionaries with flattened keys.

        This method takes a list of nested JSON objects and converts each JSON object into a
        dictionary with keys representing the nested structure using a specified separator. 
        Optionally, you can provide a list of keys to exclude or include in the resulting dictionaries.

        Example:
            Input JSON list:
            ```python
            [
                {
                    "a": {"b": {"c": "d"}},
                    "x": {"y": "z"}
                },
                {
                    "p": {"q": {"r": "s"}},
                    "u": {"v": "w"}
                }
            ]
            # Flattening with a "." separator:
            >>> print(JSONTransformer.flatten_list(json_list, "."))
            >>> [
            >>>     {"a.b.c": "d", "x.y": "z"},
            >>>     {"p.q.r": "s", "u.v": "w"}
            >>> ]
            ```

        Args:
            json_list (list[dict]): A list of JSON objects to flatten.
            sep (str): The separator to use for joining nested keys.
            exclude (list[str]): If specified, exclude keys that match the specified keys and include all others.
            include (list[str]): If specified, only include keys that match the specified keys.

        Returns:
            list[dict[str, any]]: A list of flattened dictionaries with keys representing the nested structure.
        """
        return [JSONTransformer.flatten(point, sep, exclude, include) for point in json_list]

    @staticmethod
    def get_headers(json_list: list[dict]) -> set[str]:
        """
        Get a complete headers list from a list of JSON objects.

        Args:
            json_list (list[dict]): A list of JSON objects to iterate over.

        Returns:
            set[str]: A set of headers.
        """
        return {key for item in json_list for key in item.keys()}

    @staticmethod
    def from_bigquery_iterator(iterator: Iterator, exclude: set[str] = {}, include: set[str] = {}) -> tuple[set, list]:
        """
        Convert a BigQuery RowIterator into a set of headers and a list of rows.

        Args:
            iterator (Iterator): A BigQuery RowIterator object.
            exclude (set[str], optional): A set of field names to exclude from the output. Defaults to an empty set.
            include (set[str], optional): A set of field names to include in the output. If provided, only these fields
                                          will be included. Defaults to an empty set.

        Returns:
            tuple[set, list]: A tuple containing a set of headers and a list of rows as dictionaries.
                              Headers are the field names included in the output, and rows are dictionaries
                              containing field values for each row in the RowIterator.

        Note:
            If both `exclude` and `include` are provided, the `include` set takes precedence.
        """
        schema = list(iterator.schema)
        headers = {field.name for field in schema if (
            include and field.name in include) or (exclude and field.name not in exclude)}

        rows = []
        for row in iterator:
            row = {field.name : row[field.name] for field in schema if field.name in headers}
            rows.append(row)
        
        return headers, rows

    @staticmethod
    def to_rows(json_list: list[dict], sep: str, exclude: list[str] = [], include: list[str] = []) -> tuple[list, set]:
        """
        Flatten a list of JSON objects into a list of rows and a set of headers.

        This method takes a list of nested JSON objects and converts each JSON object into a
        dictionary with keys representing the nested structure using a specified separator. 
        It then creates a list of rows, where each row contains the values from the flattened
        dictionaries, and a set of headers representing the unique keys of the flattened dictionaries.

        Args:
            json_list (list[dict]): The list of JSON objects to flatten.
            sep (str): The separator to use for joining nested keys.
            exclude (list[str]): If specified, exclude keys that match the specified keys and include all others.
            include (list[str]): If specified, only include keys that match the specified keys.

        Note:
            If both exclude and include are specified, exclude takes precedence.
            This is done by using the following logic:
                ```python
                if key_str in exclude or (include and key_str not in include):
                    continue
                ```

        Returns:
            tuple[list, set]: A tuple containing the list of rows and the set of headers.
        """
        rows, headers = [], set()
        # Flatten the JSON object into a list of dictionaries using the flatten method.
        for item in JSONTransformer.flatten_list(json_list, sep, exclude, include):
            rows.append(item)
            headers.update(item.keys())
        return rows, headers

    @staticmethod
    def to_csv_stringio(rows: list[dict], headers: set) -> StringIO:
        """
        Convert a list of rows into a CSV formatted StringIO object.

        This method takes a list of rows (dictionaries) and a set of headers, and writes them into
        a CSV formatted StringIO object. It can be used to create a CSV file-like object without
        creating an actual file on the filesystem.

        Args:
            rows (list[dict]): A list of rows to convert into a CSV formatted StringIO object.
            headers (set): A set of headers to use for the CSV data.

        Returns:
            StringIO: A CSV formatted StringIO object.
        """
        # Create a StringIO object to write the CSV data to.
        csv_stringio = StringIO()
        # Create a csv writer and write the rows to the StringIO object.
        writer = DictWriter(csv_stringio, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

        # Reset the StringIO object's position to the beginning
        csv_stringio.seek(0)

        return csv_stringio


class YotpoAPIWrapper:
    # NOTE: Update docstring if more methods are added to account for any added functionality outside of the defined scope.
    """
    A class for interacting with the Yotpo API to fetch app and account information, review data, and send manual review requests.

    The YotpoAPIWrapper uses the provided app_key and secret for authentication and constructs the necessary API endpoints for making requests.

    Args:
        app_key (str): The Yotpo app key for API authentication.
        secret (str): The client secret to authenticate requests.
        preferred_uid (Optional[int], optional): The user ID to use for fetching data relating to the Yotpo APP. Defaults to None.

    Raises:
        Exception: If either app_key or secret is not provided.
    """

    # TODO: Update the explanation of the preferred_uid argument to be more accurate/helpful.

    def __init__(self, app_key: str, secret: str, preferred_uid: Optional[int] = None) -> None:
        if not app_key or not secret:
            raise Exception(
                f"app_key(exists={bool(app_key)}) and secret(exists={bool(secret)}) are required.")

        self._app_key = app_key
        self._secret = secret
        self.user_id = preferred_uid

    async def __aenter__(self):
        self.aiohttp_session = aiohttp.ClientSession()
        self._utoken = await self._get_user_token()

        self.app_endpoint = f"https://api.yotpo.com/v1/apps/{self._app_key}/reviews?utoken={self._utoken}&"
        self.widget_endpoint = f"https://api.yotpo.com/v1/widget/{self._app_key}/reviews?utoken={self._utoken}&"
        self.write_user_endpoint = f"https://api-write.yotpo.com/users/me?utoken={self._utoken}"
        self.write_app_endpoint = f"https://api-write.yotpo.com/apps"

        if self.user_id is None:
            self.user_id = (await self.get_user())['id']

        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.aiohttp_session.close()

    async def _get_user_token(self) -> str:
        """
        Get the user access token using the provided secret.

        Args:
            app_key (str): The target app you want authenticated.
            secret (str): The client secret to authenticate the request.

        Returns:
            str: The user access token.

        Raises:
            FailedToGetTokenError: If the response status is not OK (200).
        """
        url = "https://api.yotpo.com/oauth/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": self._app_key,
            "client_secret": self._secret,
        }

        return (await self._post_request(url, data=data, parser=lambda x: x["access_token"], exception_type=FailedToGetTokenError))

    async def _options_request(self, url: str, method: str) -> bool:
        """
        Asynchronously sends a preflight OPTIONS request to check if the given method is allowed for the given endpoint.

        Args:
            url (str): The API endpoint URL.

        Returns:
            bool: True if the method is allowed, False otherwise.
        """
        if not hasattr(self, 'aiohttp_session'):
            raise SessionNotCreatedError()

        # Cheeky way to make sure `method` is at least syntactically correct.
        if (method := method.upper()) not in ['GET', 'HEAD', 'POST', 'PUT', 'DELETE', 'CONNECT', 'OPTIONS', 'TRACE']:
            raise PreflightException(method)

        async with self.aiohttp_session.options(url) as response:
            if response.ok:
                allowed_methods = response.headers.get("Allow", "")
                return method in allowed_methods.split(", ")
            raise Exception(
                f"Error: {response.status} {response.reason} - {url}")

    async def _get_request(self, url: str, parser: Callable[[dict], dict] = None, exception_type=CustomException, **kwargs) -> dict:
        """
        Asynchronously sends a GET request to the specified URL and parses the response using the provided parser.

        Args:
            session (aiohttp.ClientSession): An aiohttp client session for making requests.
            url (str): The URL to send the request to.
            parser (Callable[[dict], dict]): A function to parse the response JSON. Defaults to json.loads.
            exception_type (Union[CustomException, Exception]): The type of exception to raise when an error occurs. Defaults to CustomException.
            **kwargs: Additional keyword arguments to be passed to the request object.

        Returns:
            dict: The parsed JSON response.

        Raises:
            SessionNotCreatedError: If the aiohttp session has not been created.
            exception_type: If the response status is not 200, an instance of the specified exception_type is raised with an error message.
        """
        if not hasattr(self, 'aiohttp_session'):
            raise SessionNotCreatedError()

        async with self.aiohttp_session.get(url, **kwargs) as response:
            if response.status == 200:
                raw_data = json_loads(await response.read())
                if parser is None:
                    return raw_data
                return parser(raw_data)
            raise exception_type(
                f"Error: {response.status} | {response.reason} - {url}")

    async def _post_request(self, url: str, data: dict, parser: Callable[[dict], dict] = None, exception_type: Exception = CustomException, **kwargs) -> dict:
        """
        Asynchronously sends a POST request to the specified URL and parses the response using the provided parser.

        Args:
            session (aiohttp.ClientSession): An aiohttp client session for making requests.
            url (str): The URL to send the request to.
            data (dict): The data to send in the request body.
            parser (Callable[[dict], dict]): A function to parse the response JSON. Defaults to json.loads.
            exception_type (Union[CustomException, Exception]): The type of exception to raise when an error occurs. Defaults to CustomException.
            **kwargs: Additional keyword arguments to be passed to the request object.

        Returns:
            dict: The parsed JSON response.

        Raises:
            SessionNotCreatedError: If the aiohttp session has not been created.
            exception_type: If the response status is not 200, an instance of the specified exception_type is raised with an error message.
        """
        if not hasattr(self, 'aiohttp_session'):
            raise SessionNotCreatedError()

        async with self.aiohttp_session.post(url, data=data, **kwargs) as response:
            if response.status == 200:
                raw_data = json_loads(await response.read())
                if parser is None:
                    return raw_data
                return parser(raw_data)
            raise exception_type(
                f"Error: {response.status} | {response.reason} - {url}")

    async def _pages(self, endpoint: str, start_page: int = 1) -> AsyncGenerator[tuple[str, int], None]:
        """
        Asynchronously generate URLs for each page of results.

        Args:
            endpoint (str): The API endpoint to query.
            start_page (int): The first page to generate.

        Yields:
            tuple[str, int] ->
                str: The URL for the next page of results.
                int: The current page number.
        """
        last_page = ceil((await self.get_total_reviews()) / 100)
        is_widget = "widget" in endpoint
        for num in range(start_page, last_page + 1):
            yield endpoint + urlencode(({("per_page" if is_widget else "count"): 100, "page": num})), num

    async def get_user(self) -> dict:
        """
        Asynchronously fetches user data from the user endpoint.

        This function returns various user-related data, such as name, email, display_name, company, position, and other
        metadata like sign_in_count and package details. This data can be useful for understanding user profiles,
        managing access, or customizing the user experience based on the retrieved information.

        Returns:
            dict: A dictionary containing user information, including personal details, activity statistics, and package details.


        Example:
            ```python
            >>> yotpo = YotpoAPIWrapper(app_key, secret)
            >>> async with yotpo as yp:
            >>>     user = await yp.get_user()
            ```

        Raises:
            UserNotFound: If the request to the user endpoint returns a non-OK status.
        """
        url = self.write_user_endpoint

        return await self._get_request(url, parser=lambda data: data['response']['user'], exception_type=UserNotFound)

    async def get_app(self) -> dict:
        """
        Asynchronously fetches app data for the app associated with the preferred user ID.

        Returns:
            dict: A dictionary containing app information derived from the specified user. The app information includes
            details such as app_key, domain, name, url, account type, users, reminders, custom design, created
            and updated timestamps, tracking code status, account emails, package details, enabled product
            categories, features usage summary, data for events, category, installed status, organization,
            and associated apps.

        Raises:
            AppDataNotFound: If the request to the app endpoint returns a non-OK status.
        """
        # NOTE: The user_id does not appear to actually matter at all, as it still returns data even if it is not a valid user ID.
        # I suspect `user_id` is just used to track which user is making the request.

        url = f"https://api-write.yotpo.com/apps/{self._app_key}?user_id={self.user_id}&utoken={self._utoken}"

        return await self._get_request(url, parser=lambda data: data['response']['app'], exception_type=AppNotFound)

    async def get_total_reviews(self) -> int:
        """
        Asynchronously fetches the total number of reviews for an app.

        This method first retrieves the user and their user ID, then fetches the app data
        associated with that user. Finally, it returns the total number of reviews for the app.

        Returns:
            int: The total number of reviews for the app.

        Raises:
            AppDataNotFound: If unable to get the app data.
        """
        app = await self.get_app()

        return app['data_for_events']['total_reviews']

    async def get_templates(self) -> dict:
        """
        Asynchronously fetch the email templates associated with the Yotpo account.

        This method retrieves the app data, extracts the email templates, and returns them as a dictionary.
        The primary use case for this function is to obtain the template IDs required for the 'send_review_request' method.

        The returned dictionary contains template objects with properties such as 'id' (template_id),
        'email_type', 'data', and others. For example:
        ```python
            {
                'id': 10291872,
                'account_id': 9092811,
                'email_type_id': 31,
                'email_type': {
                    'id': 31,
                    'name': 'mail_post_service',
                    'template': 'testimonials_request'
                },
                'data': {
                    'subject': 'Tell us what you think - {company_name}',
                    'header': 'Hello {user},\\n We love having you as a customer and would really appreciate it if you filled out the form below.',
                    'bottom': 'Thanks so much! <br> - {company_name}'
                },
                'formless_call_to_action': '',
                'days_delay': 3,
                'email_submission_type_id': 9
            }
        ```
        Returns:
            dict: A dictionary containing the email templates, keyed by their names.
        """
        app = await self.get_app()
        templates = app['account_emails']
        return templates

    async def fetch_review_page(self, url: str) -> list[dict]:
        """
        Asynchronously fetch a single review page from the specified URL.

        This function fetches review data from the provided URL and parses the response based on whether
        the URL is for the widget endpoint or not.

        Args:
            url (str): The URL from which to fetch review data.

        Returns:
            list[dict]: A list of review dictionaries.

        """
        is_widget = "widget" in url
        return await self._get_request(url, parser=lambda data: data['response']['reviews'] if is_widget else data['reviews'])

    async def fetch_all_reviews(self, published: bool = True) -> list[dict]:
        """
        Asynchronously fetch all reviews from the specified endpoints.

        This function fetches review data from the app and widget endpoints if `published` is set to True, 
        and only from the app endpoint if `published` is set to False. The fetched reviews are then merged
        based on their "id" attribute.

        Args:
            published (bool, optional): Determines if the function should fetch reviews from both
                                         the app and widget endpoints or just the app endpoint.
                                         Defaults to True.

        Returns:
            list[dict]: A list of merged review dictionaries.

        Raises:
            Exception: If one or more requests fail.
        """
        reviews = []
        for endpoint in ([self.app_endpoint, self.widget_endpoint] if published else [self.app_endpoint]):
            review_requests = []
            async for url, _ in self._pages(endpoint):
                task = asyncio.create_task(self.fetch_review_page(url))
                review_requests.append(task)

            print(
                f"Gathering {len(review_requests)} review requests from {endpoint}...")
            results = await asyncio.gather(*review_requests, return_exceptions=True)

            if any([isinstance(result, Exception) for result in results]):
                raise Exception("One or more requests failed.")
            else:
                # Flatten the list of lists into one big list using itertools.chain
                results = list(chain.from_iterable(results))
                reviews.append(results)

        return JSONTransformer.merge_on_key("id", *reviews)

    async def send_review_request(self, template_id: int | str, csv_stringio: StringIO, spam_check: bool = False):
        """
        Asynchronously send a "manual" review request to Yotpo using a specific email template.

        This method takes a template_id, a CSV formatted StringIO object containing the review
        request data, and an optional spam_check flag. It uploads the review data to Yotpo and
        sends the review request using the specified email template.

        Args:
            template_id (int | str): The ID of the email template to use for the review request.
            csv_stringio (StringIO): A StringIO object containing the review request data in CSV format.
                                     (Use the `JSONTransformer` class to generate this.)
            spam_check (bool, optional): Whether or not to enable the built-in Yotpo spam check. Defaults to False.

        Returns:
            dict: A dictionary containing the response data.

        Raises:
            Exception: If any response status is not 200.
            UploadException: If the uploaded file is not valid.
            SendException: If the send request fails.
        """
        upload_url = f"{self.write_app_endpoint}/{self._app_key}/account_emails/{template_id}/upload_mailing_list"
        send_url = f"{self.write_app_endpoint}/{self._app_key}/account_emails/{template_id}/send_burst_email"

        upload_response = await self._post_request(upload_url, {"file": csv_stringio, "utoken": self._utoken}, exception_type=UploadException)
        upload_data = upload_response['response']['response']

        if upload_data['is_valid_file']:
            send_response = await self._post_request(send_url, {"file_path": upload_data['file_path'], "utoken": self._utoken, "activate_spam_limitations": spam_check}, exception_type=SendException)
        else:
            raise UploadException("Error: Uploaded file is not valid")

        return {"upload": upload_response, "send": send_response}
