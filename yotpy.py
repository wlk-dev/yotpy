import asyncio
import aiohttp
from csv import DictWriter
from io import StringIO
from requests import post as requests_post
from typing import AsyncGenerator, Iterator, Union
from html import unescape
from urllib.parse import urlencode
from math import ceil
from itertools import chain


class PreflightException(Exception):
    pass

class UploadException(Exception):
    pass

class SendException(Exception):
    pass



# TODO: Refactor naming for the below two classes
class UserIdNotFound(Exception):
    """Raised when the user ID cannot be retrieved."""
    pass

class AppDataNotFound(Exception):
    """Raised when the app data cannot be retrieved."""
    pass

# TODO: More insightful sentiment analysis
# TODO: Give better error messages for bad requests

class JSONTransformer:
    """
    A utility class for transforming JSON data into various formats.
    """

    @staticmethod
    def _stream_json_data(json: dict, keys: tuple) -> Iterator[dict[str, Union[tuple, str]]]:
        """
        Stream the full key directory and its final value of a JSON object of arbitrary depth, including lists.
        
        This method recursively traverses a nested JSON object and yields dictionaries containing the full key
        directory (as tuples) and their associated values. It handles dictionaries and lists within the JSON object.

        Example:
            Input JSON:
            ```json
            { "a": 
                { "b":
                    [ { "c": "d" }, { "e": [1,2,3] } ]
                },
            "x": { "y": "z" }
            }
            ```
            Streaming key directories and values:
            ```python
            >>> print(list(JSONTransformer._stream_json_data(json, tuple())))
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
                for result in JSONTransformer._stream_json_data(value, keys + (key,)):
                    yield result
            elif isinstance(value, list):
                # If the value is a list, iterate through the list and recursively
                # call this method with the updated keys "list" and index as an additional key.
                for index, item in enumerate(value):
                    if not isinstance(item, (dict, list)):
                        yield {'keys': keys + (key, str(index)), 'value': item}
                        continue
                        
                    for result in JSONTransformer._stream_json_data(item, keys + (key, str(index))):
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
            ```json
            { "a": 
                { "b":
                    [ { "c": "d" }, { "e": [1,2,3] } ]
                },
                "x": { "y": "z" }
            }
            ```
            Flattening with a "." separator:
            ```python
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

        # Stream the key-value pairs of the JSON object using the _stream_json_data method.
        data = {}
        for item in JSONTransformer._stream_json_data(json, tuple()):
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
            ```json
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
            ```
            Flattening with a "." separator:
            ```python
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
                ```
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
    #NOTE: Update docstring if more methods are added to account for any added functionality outside of the defined scope.
    """
    A class for interacting with the Yotpo API to fetch app and account information, review data, and send manual review requests.

    The YotpoAPIWrapper uses the provided app_key and secret for authentication and constructs the necessary API endpoints for making requests.

    Args:
        app_key (str): The Yotpo app key for API authentication.
        secret (str): The client secret to authenticate requests.

    Raises:
        Exception: If either app_key or secret is not provided.
    """
    def __init__(self, app_key: str, secret: str) -> None:
        if not app_key or not secret:
            raise Exception(f"app_key(exists={bool(app_key)}) and secret(exists={bool(secret)}) are required.")
        
        self._app_key = app_key
        self._utoken = YotpoAPIWrapper._get_user_token(app_key, secret)
        self.app_endpoint = f"https://api.yotpo.com/v1/apps/{app_key}/reviews?utoken={self._utoken}&"
        self.widget_endpoint = f"https://api.yotpo.com/v1/widget/{app_key}/reviews?utoken={self._utoken}&"
        self.write_user_endpoint = f"https://api-write.yotpo.com/users/me?utoken={self._utoken}"
        self.write_app_endpoint = f"https://api-write.yotpo.com/apps"

    @staticmethod
    def _get_user_token(app_key: str, secret: str) -> str:
        """
        Get the user access token using the provided secret.

        Args:
            app_key (str): The target app you want authenticated.
            secret (str): The client secret to authenticate the request.

        Returns:
            str: The user access token.

        Raises:
            Exception: If the response status is not OK (200).
        """
        url = "https://api.yotpo.com/oauth/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": app_key,
            "client_secret": secret,
        }
        # TODO: Replace with aiohttp
        response = requests_post(url, data=data)
        if response.ok:
            return response.json()["access_token"]

        raise Exception(f"Error: {response.status_code} | {response.reason} - {url}")

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

    # TODO: The use of this method is deprecated. Functionality will be replaced `get_total_reviews`. Possible refactor of this method for better abstractions.
    # NOTE: Keep for reference until major refactoring is complete.
    # Use of this function is discouraged. `get_total_reviews` is just better all on metrics.
    async def get_total_pages(self) -> int:
        """
        Asynchronously get the total number of pages of results.

        Returns:
            int: The total number of pages.
        """
        url = self.widget_endpoint + urlencode({"per_page": 1, "page": 1})
        async with aiohttp.request("GET", url) as response:
            if response.ok:
                pagination = (await response.json())['response']['pagination']
                return ceil(pagination['total'] / 100)

            raise Exception(
                f"Error: {response.status} {response.reason} - {url}")
    
    async def get_total_reviews(self) -> int:
        """
        Asynchronously fetches the total number of reviews for an app.

        This method first retrieves the user and their user ID, then fetches the app data
        associated with that user. Finally, it returns the total number of reviews for the app.

        Returns:
            int: The total number of reviews for the app.

        Raises:
            UserIdNotFound: If unable to get the user ID.
            AppDataNotFound: If unable to get the app data.
        """
        if user_id := (await self.get_user()).get("id") is None:
            raise UserIdNotFound("Error: Unable to get user id.")

        app = await self.get_app(user_id)

        return app['data_for_events']['total_reviews']
 
    async def _preflight_request(self, url: str, method : str) -> bool:
        """
        Asynchronously sends a preflight OPTIONS request to check if the given method is allowed for the given endpoint.

        Args:
            url (str): The API endpoint URL.

        Returns:
            bool: True if the method is allowed, False otherwise.
        """
        # Cheeky way to make sure `method` is at least syntactically correct.
        if (method := method.upper()) not in ['GET', 'HEAD', 'POST', 'PUT', 'DELETE', 'CONNECT', 'OPTIONS', 'TRACE']:
            raise Exception("Invalid HTTP method: ", method)

        async with aiohttp.request(method, url) as response:
            if response.ok:
                allowed_methods = response.headers.get("Allow", "")
                return "POST" in allowed_methods.split(", ")
            raise Exception(f"Error: {response.status} {response.reason} - {url}")

    async def get_user(self) -> dict:
        """
        Asynchronously fetches user data from the user endpoint.

        This function returns various user-related data, such as name, email, display_name, company, position, and other
        metadata like sign_in_count and package details. This data can be useful for understanding user profiles,
        managing access, or customizing the user experience based on the retrieved information.

        Returns:
            dict: A dictionary containing user information, including personal details, activity statistics, and package details.

        Note:
            Mostly you will only need to retrieve the id from the user endpoint, and then use it to fetch the app.

        Example:
            ```python
            >>> yotpo = YotpoAPIWrapper(app_key, secret)
            >>> user_id = (await yotpo.get_user())['id']
            >>> app = await yotpo.get_app(user_id)
            ```

        Raises:
            Exception: If the request to the user endpoint returns a non-OK status.
        """
        url = self.write_user_endpoint
        
        async with aiohttp.request("GET", url) as response:
            if response.ok:
                return (await response.json())['response']['user']
            raise Exception(f"Error: {response.status} | {response.reason} - {url}")

    async def get_app(self, user_id: str | int) -> dict:
        """
        Asynchronously fetches app data for the app associated with the given user ID.

        Args:
            user_id (str | int): The user ID for which to fetch app data.

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

        # TODO: If the above is true, we can probably abstract away the `user_id` parameter and just use the user ID from the user endpoint.
        # And if need be we can add some configuration to allow the user to specify a user ID if they want to.

        url = f"https://api-write.yotpo.com/apps/{self._app_key}?user_id={user_id}&utoken={self._utoken}"

        async with aiohttp.request("GET", url) as response:
            if response.ok:
                return (await response.json())['response']['app']
            raise AppDataNotFound(f"Error: {response.status} | {response.reason} - {url}")
        
    async def fetch(self, session: aiohttp.ClientSession, url: str, page_num : int) -> list[dict]:
        """
        Asynchronously fetch the contents of a URL.

        Args:
            session (aiohttp.ClientSession): The aiohttp client session to use for the request.
            url (str): The URL to fetch.
            page_num (int): For tracking purposes.

        Returns:
            list[dict]: A list of reviews.

        Raises:
            Exception: If the response status is not 200.
        """
        # TODO: Generalize this, so that it can be used for other endpoints.
        # TODO: Add return parser function as an argument.
        print(f"Fetching URL: {url}")
        is_widget = "widget" in url
        # TODO: remove "page_number" metadata from reviews.
        # It's use is now deprecated. Remove when convenient.
        async with session.get(url) as response:
            if response.status == 200:
                json = await response.json()
                page = json['response']['reviews'] if is_widget else json['reviews']
                return [dict(review, **{"page_number": page_num}) for review in page]

            raise Exception(
                f"Error: {response.status} {response.reason} - {url}")

    async def batch_fetch(self, endpoint: str, start_page : int = 1) -> list[dict]:
        """
        Asynchronously fetch all pages of results for an API endpoint.

        Args:
            endpoint (str): The API endpoint to query.
            start_page (int): The first page to generate.

        Returns:
            list: A list of response json and an integer indicating the number of pages queried.

        Raises:
            Exception: If any response status is not 200.
        """
        async with aiohttp.ClientSession() as session:
            tasks = []
            async for url, num in self._pages(endpoint, start_page):
                task = asyncio.create_task(self.fetch(session, url, num))
                tasks.append(task)
            results = await asyncio.gather(*tasks, return_exceptions=True)
            if any([isinstance(result, Exception) for result in results]):
                raise Exception("One or more requests failed.")
            else:
                # Flatten the list of lists into one big list using itertools.chain
                results = list(chain.from_iterable(results))
                return results

    async def _post_request(self, session, url, data, as_form_data=False):
        """
        Send an asynchronous POST request to the specified URL with the given data using the provided aiohttp session, data will be formatted as `aiohttp.FormData` if specified .

        Args:
            session (aiohttp.ClientSession): The aiohttp client session to use for sending the request.
            url (str): The URL to send the POST request to.
            data (dict): A dictionary containing the data to be sent in the request.
            as_form_data (bool, optional): Whether or not to format the data as `aiohttp.FormData`. Defaults to False.

        Returns:
            dict: A dictionary containing the JSON response data.

        Raises:
            Exception: If the response status code is not 200.
        """
        if as_form_data:
            form_data = aiohttp.FormData()
            for key, value in data.items():
                form_data.add_field(key, value)

        async with session.post(url, data=data) as response:
            if response.status == 200:
                return await response.json()
            else:
                raise Exception(f"Error: {response.status} {response.reason} - {url}")
    
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
        """
        upload_url = f"{self.write_app_endpoint}/{self._app_key}/account_emails/{template_id}/upload_mailing_list"
        send_url = f"{self.write_app_endpoint}/{self._app_key}/account_emails/{template_id}/send_burst_email"

        async with aiohttp.ClientSession() as session:
            # Upload data needs to be sent as form data.
            upload_response = await self._post_request(session, upload_url, {"file": csv_stringio, "utoken": self._utoken}, as_form_data=True)
            upload_data = upload_response['response']['response']

            if upload_data['is_valid_file']:
                send_response = await self._post_request(session, send_url, {"file_path": upload_data['file_path'], "utoken": self._utoken, "activate_spam_limitations": spam_check})
            else:
                raise UploadException("Error: Uploaded file is not valid")

        return {"upload": upload_response, "send": send_response}
