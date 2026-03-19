import requests
import logging

from hh.config import BASE_URL, DEFAULT_HEADERS


def fetch_vacancy_with_payload(vacancy_id: int) -> dict | None:
    response = requests.get(
        f"{BASE_URL}/{vacancy_id}",
        headers=DEFAULT_HEADERS,
        verify=False,
        timeout=10,
    )
    if response.status_code != 200:
        logging.warning(
            f"Failed to fetch vacancy {vacancy_id}, status={response.status_code}"
        )
        return None

    return response.json()

def fetch_vacancy_ids(role_id: int, page: int) -> dict | None:
    response = requests.get(
        BASE_URL,
        params={
            "professional_role": role_id,
            "per_page": 100,
            "page": page,
        },
        headers=DEFAULT_HEADERS,
        verify=False,
        timeout=15,
    )

    if response.status_code != 200:
        logging.warning(
            f"HH API error {response.status_code}, role={role_id}, page={page}"
        )
        return None
    return response.json()