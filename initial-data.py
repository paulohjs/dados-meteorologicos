from datetime import datetime
from meteostat import Point, Hourly
import pandas as pd
from time import sleep
import json
from geopy.geocoders import Nominatim

triangulo_mineiro = [
    "Uberlândia", 
    "Uberaba", 
    "Araguari", 
    "Ituiutaba", 
    "Patos de Minas", 
    "Montes Claros", 
    "Conceição das Alagoas", 
    "Ibiá", 
    "Campo Florido", 
    "Centralina", 
    "Frutal", 
    "Iturama", 
    "Paracatu", 
    "Tupaciguara", 
    "Prata", 
    "Cascalho Rico", 
    "Santa Vitória", 
    "Indianópolis", 
    "Nova Ponte", 
    "Estrela do Sul", 
    "Campos Altos", 
    "Santa Juliana", 
    "Perdizes", 
    "Patrocínio", 
    "Cáceres", 
    "São Gotardo", 
    "Pedrinópolis", 
    "Pedra do Indaiá", 
    "Douradoquara"
]


def get_geolocation(country_name: str, city_name: str):
    # Crie um objeto geocodificador
    geolocator = Nominatim(user_agent="dados-meteorologicos_paulo.hjs@hotmail.com")

    # Nome do local que você quer pesquisar
    local = f"{city_name}, {country_name}"

    # Realiza a geocodificação
    localizacao = geolocator.geocode(local)

    # Verifica se a geocodificação foi bem-sucedida
    if localizacao:
        latitude = localizacao.latitude
        longitude = localizacao.longitude

        # Prepara o dado para ser salvo
        data = {local: {'latitude': latitude, 'longitude': longitude}}

        # Lê o arquivo JSON existente, se houver
        try:
            with open('cities_geocode.json', 'r', encoding='utf-8') as cities_geocode:
                all_data = json.load(cities_geocode)
        except (FileNotFoundError, json.JSONDecodeError):
            # Se o arquivo não existe ou está vazio, inicializa a lista
            all_data = []

        # Adiciona os novos dados
        all_data.append(data)

        # Escreve os dados no arquivo novamente
        with open('cities_geocode.json', 'w', encoding= 'utf-8') as cities_geocode:
            json.dump(all_data, cities_geocode, indent=4)

        print(f"Local {local} geocodificado com sucesso!")
    else:
        print(f"Local {local} não encontrado.")


def main():

    for city in triangulo_mineiro:
        get_geolocation('Brasil', city)
        sleep(30)

    
    # defining variables
    start = datetime(2021, 1, 1, 20) #iniciando onde tem dados de todas as colunas de todas as cidades
    end = datetime(2024, 11, 15)
    df_weather_data = pd.DataFrame
    cities_geocode_file = 'cities_geocode.json'

    # Get cities geocode from json
    with open(cities_geocode_file, 'r', encoding='utf-8') as cities_geocode_json:
        cities_geocode = json.load(cities_geocode_json)

    
    # Get weather data by city
    for city in cities_geocode:
        # Setting latitude and longitude by city
        for name, coords in city.items():
            print(f'Cidade: {name}, Latitude: {coords["latitude"]}')
            latitude = coords['latitude']
            longitude = coords['longitude']

        # Seting variable Point by city
        point = Point(latitude, longitude)
        # Getin hourly weather data by city
        data = Hourly(point, start, end)
        data = data.fetch()

        if not data.empty:
            # Transform index (time) in simple column
            data = data.reset_index()
            # Add column with name of city
            data['city'] = name
            data = data[['city', 'time','temp', 'rhum', 'prcp', 'wspd']]
            # concat city df on amount df
            if df_weather_data.empty:
                df_weather_data = data
            else:
                df_weather_data = pd.concat([df_weather_data, data])
                df_weather_data = df_weather_data.reset_index(drop=True)

    
    df_weather_data.to_parquet('weather_data.parquet')
      


if __name__ == '__main__':
    main()
