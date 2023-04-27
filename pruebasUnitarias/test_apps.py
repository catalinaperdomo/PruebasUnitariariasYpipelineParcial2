import datetime
from apps import download, get_file_name, url


def test_apps_download(mocker):
    mocker.patch("requests.get", return_value=True)
    html = download('https://www.eltiempo.com')
    assert html


def test_apps_get_file_name():
    now = datetime.datetime.now()
    file_name = now.strftime("%Y-%m-%d")+".html"
    assert get_file_name() == file_name


def test_apps_get_url():
    assert url() == 'https://www.eltiempo.com'
