from confluent_kafka import Consumer

# Configuración del consumidor
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'movie_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)
consumer.subscribe(['dbserver_movies.db_movies_netflix_transact.movie'])

try:
    while True:
        msg = consumer.poll(1.0)  # Espera mensajes con un timeout de 1s
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        # Procesa el mensaje
        data = msg.value().decode('utf-8')
        import json
        parsed = json.loads(data)
        payload = parsed.get("payload", {})
        before = payload.get("before", None)
        after = payload.get("after", None)
        op = payload.get("op", None)
        
        print(f"Operation: {op}")
        print(f"Before: {before}")
        print(f"After: {after}")
finally:
    consumer.close()
