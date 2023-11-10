import time
from kafka import KafkaProducer
import requests
import logging

# Configuration du logger
logging.basicConfig(level=logging.INFO)

# Configuration du producteur Kafka
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: str(v).encode('utf-8'))

# Fonction pour envoyer des données au topic Kafka
def send_movie_data_to_kafka(movie_data):
    try:
        producer.send('movielens-data', movie_data)
        producer.flush()
        logging.info(f'Données envoyées : {movie_data}')
    except Exception as e:
        logging.error(f'Erreur lors de l\'envoi des données : {str(e)}')

# Simulation de la collecte de données MovieLens toutes les 2 secondes
while True:  
    API_KEY = 'c4811c11f77eae493e5cf84c2836036d'
    MOVIE_ENDPOINT = "https://api.themoviedb.org/3/movie/popular?api_key={}&language=en-US&page=1".format(API_KEY)

    # Effectuez la requête HTTP pour obtenir les données météorologiques actuelles
    response = requests.get(MOVIE_ENDPOINT)
    
    if response.status_code == 200:
        movie_data = response.json()
        send_movie_data_to_kafka(movie_data)
        time.sleep(2)  # Pause de 2 secondes entre les envois

# Close the Kafka producer after use
producer.close()
