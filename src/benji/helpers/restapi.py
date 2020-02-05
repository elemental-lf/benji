from typing import Any, Dict, Tuple, List

import requests


class BenjiRESTClient:

    CORE_API_VERSION_V1 = 'v1'
    CORE_API_GROUP = 'core'

    def __init__(self, api_endpoint: str):
        self._api_endpoint = api_endpoint

    def _api_request(self,
                     path: str,
                     method: str = 'GET',
                     body: Any = None,
                     params: Dict[str, Any] = None,
                     timeout: Tuple[int, int] = (2, 30),
                     api_version: str = CORE_API_VERSION_V1,
                     api_group=CORE_API_GROUP) -> Dict[str, Any]:
        response = requests.request(method,
                                    f'{self._api_endpoint}/apis/{api_group}/{api_version}/{path}',
                                    headers={'Content-Type': 'application/json; charset=utf-8'},
                                    params=params,
                                    json=body,
                                    timeout=timeout)
        response.raise_for_status()

        if response.status_code not in (200, 201, 204):
            raise requests.HTTPError(f'API call failed with status code {response.status_code}.', response=response)

        return response.json()

    def get_version_by_uid(self, version_uid: str = None) -> Dict[str, Any]:
        return self._api_request(f'versions/{version_uid}')['versions'][0]

    def find_versions_with_filter(self,
                                  filter_expression: str = None,
                                  include_blocks: bool = False) -> List[Dict[str, Any]]:
        return self._api_request('versions',
                                 params={
                                     'filter_expression': filter_expression,
                                     'include_blocks': include_blocks
                                 })['versions']

    def rm(self,
           version_uid: str,
           force: bool = False,
           keep_metadata_backup: bool = False,
           override_lock: bool = False):
        return self._api_request(f'versions/{version_uid}',
                                 method='DELETE',
                                 params={
                                     'force': force,
                                     'keep_metadata_backup': keep_metadata_backup,
                                     'override_lock': override_lock
                                 })['versions'][0]

    def protect(self, version_uid: str, protected: bool):
        return self._api_request(f'versions/{version_uid}', method='PATCH',
                                 params={'protected': protected})['versions'][0]
