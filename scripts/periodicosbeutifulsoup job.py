import boto3
from datetime import date
from bs4 import BeautifulSoup

S3_BUCKET = 'periodicospc2'
date_day = date.today()

s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')


def extract_tiempo(file):
    """
    Extrae la información de El Tiempo y guarda los datos en S3.
    """
    contenido = s3.get_object(Bucket=S3_BUCKET, Key=file)["Body"].read()
    soup = BeautifulSoup(contenido, 'html.parser')

    headers = 'titulo,categoria,link\n'

    for articles in soup.find_all('article'):
        for links in articles.find_all('a', class_='title page-link'):
            try:
                link = '"' + links["href"] + '"'
                categoria = '"' + link.split('/')[1] + '"'
                titulo = '"' + (links.text) + '"'
                headers += f'{titulo},{categoria},{link}\n'
            except Exception:
                pass

    url = (
        "headlines/final/periodico=eltiempo/year="
        + str(date_day.year)
        + "/month="
        + str(date_day.strftime('%m'))
        + "/day="
        + str(date_day.strftime('%d'))
        + "/elTiempo"
    )
    s3_resource.Object(
        S3_BUCKET,
        url + '{}.csv'.format(date_day.strftime('%Y-%m-%d'))
    ).put(Body=headers)


def extract_espectador(file):
    """
    Extrae la información de El Espectador y guarda los datos en S3.
    """
    contenido = s3.get_object(Bucket=S3_BUCKET, Key=file)["Body"].read()
    soup = BeautifulSoup(contenido, 'html.parser')

    headers = 'titulo,categoria,link\n'

    for articles in soup.find_all('h2', class_='Card-Title Title Title'):
        for links in articles.find_all('a'):
            try:
                link = links["href"]
                categoria = link.split('/')[1]
                titulo = ((link.replace('-', ' ')).replace(
                    categoria, '')).replace('/', '')
                headers += f'{titulo},{categoria},{link}\n'
            except Exception:
                pass

    url = (
        "headlines/final/periodico=elespectador/year="
        + str(date_day.year)
        + "/month="
        + str(date_day.strftime('%m'))
        + "/day="
        + str(date_day.strftime('%d'))
        + "/elEspectador"
    )
    s3_resource.Object(
        S3_BUCKET,
        url + '{}.csv'.format(date_day.strftime('%Y-%m-%d'))
    ).put(Body=headers)


extract_tiempo(
    "headlines/raw/El_Tiempo-"
    + str(date_day.year)
    + "-"
    + str(date_day.strftime('%m'))
    + "-"
    + str(date_day.strftime('%d'))
    + ".html"
)
extract_espectador(
    "headlines/raw/El_Espectador-"
    + str(date_day.year)
    + "-"
    + str(date_day.strftime('%m'))
    + "-"
    + str(date_day.strftime('%d'))
    + ".html"
)
