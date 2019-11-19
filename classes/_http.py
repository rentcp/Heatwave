import requests


class SessionWithHeaderRedirection(requests.Session):
    """Overrides requests.Session.rebuild_auth to maintain headers when redirected"""

    AUTH_HOST = 'urs.earthdata.nasa.gov'

    def __init__(self, username, password):
        super().__init__()
        self.auth = (username, password)

    def rebuild_auth(self, prepared_request, response):
        headers = prepared_request.headers
        url = prepared_request.url

        if 'Authorization' in headers:
            original = requests.utils.urlparse(response.request.url).hostname
            redirect = requests.utils.urlparse(url).hostname

            if original != redirect and redirect != self.AUTH_HOST and original != self.AUTH_HOST:
                del headers['Authorization']
        return

