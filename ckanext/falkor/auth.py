import logging
import requests
import time
from typing import Union

log = logging.getLogger(__name__)


class Credentials:
    client_id: str
    client_secret: str
    username: str
    password: str

    def __init__(
        self, client_id: str, client_secret: str, username: str, password: str
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password


class Token:
    token: str
    expires_in: int

    def __init__(self, token: str, expires_in: int):
        self.token = token
        self.expires_in = expires_in


class Auth:
    __access_token: Union[Token, None]
    __refresh_token: Union[Token, None]
    __credentials: Credentials
    __timestamp: float
    __endpoint: str

    def __init__(self, credentials: Credentials, endpoint: str):
        self.__credentials = credentials
        self.__access_token = None
        self.__refresh_token = None
        self.__timestamp = 0
        self.__endpoint = endpoint

    @property
    def access_token(self) -> str:
        if self.__access_token is None or self.__refresh_token is None:
            log.debug("No tokens, logging in...")
            self.__login()
        elif self.__is_token_expired(self.__refresh_token):
            log.debug("Refresh token expired, reauthenticating...")
            self.__login()
        elif self.__is_token_expired(self.__access_token):
            log.debug("Access token expired, refreshing...")
            self.__refresh()
        return self.__access_token.token

    def __is_token_expired(self, token: Token) -> bool:
        expires_at = self.__timestamp + token.expires_in
        current_time = time.time()
        log.debug(
            "TOKEN EXPIRE INFO: Expires at: "
            + str(expires_at)
            + " - Current Time: "
            + str(current_time)
        )
        return current_time >= expires_at

    def __login(self) -> None:
        request = {
            "client_id": self.__credentials.client_id,
            "client_secret": self.__credentials.client_secret,
            "username": self.__credentials.username,
            "password": self.__credentials.password,
            "grant_type": "password",
        }
        response = requests.post(self.__endpoint, request, timeout=10)
        body = response.json()
        self.__set_token(body)

    def __refresh(self) -> None:
        request = {
            "grant_type": "refresh_token",
            "client_id": self.__credentials.client_id,
            "client_secret": self.__credentials.client_secret,
            "refresh_token": self.__refresh_token.token,
        }

        response = requests.post(self.__endpoint, request, timeout=10)
        body = response.json()
        self.__set_token(body)

    def __set_token(self, body) -> None:
        self.__access_token = Token(body["access_token"], body["expires_in"])
        self.__refresh_token = Token(
            body["refresh_token"], body["refresh_expires_in"])
        self.__timestamp = time.time()
