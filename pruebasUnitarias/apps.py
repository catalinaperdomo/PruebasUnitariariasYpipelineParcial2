import boto3
import requests
import datetime


def url():
    return 'https://www.eltiempo.com'


def download(url):
    response = requests.get(url)

    return response


def upload(file_name, response):
    # Guardar la página en el bucket de S3
    s3 = boto3.resource('s3')
    s3.Bucket('periodicospcdos').put_object(
        Key=file_name,
        Body=response.content
    )


def get_file_name():
    now = datetime.datetime.now()
    file_name = now.strftime("%Y-%m-%d") + ".html"

    return file_name


def f():
    file_name = get_file_name()

    response = download(url())
    upload(file_name, response)
