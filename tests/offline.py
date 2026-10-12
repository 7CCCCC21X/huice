"""Imported first by every suite: any real HTTP request fails at once, so results never depend on the network."""
import http.client
import urllib.request


def _no_network(request, *args, **kwargs):
    raise OSError(f"network disabled in tests: {getattr(request, 'full_url', request)}")


def _no_connect(self, *args, **kwargs):
    raise OSError(f"network disabled in tests: {self.host}")


urllib.request.urlopen = _no_network
http.client.HTTPSConnection.connect = _no_connect  # the kept-alive path (1.62.0) connects here
