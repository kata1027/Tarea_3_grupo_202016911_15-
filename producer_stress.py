#!/usr/bin/env python
# coding: utf-8

# In[2]:





# In[3]:


from kafka import KafkaProducer
import pandas as pd
import json
import time

# Leer tu dataset
file_path = r"D:\Documentos Angelica\cata_bigData\dreaddit-test.csv"  # Asegúrate de que la ruta sea correcta
df = pd.read_csv(file_path)


# In[4]:


# Configurar el Producer de Kafka
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


# In[ ]:


# Enviar cada registro como un mensaje a Kafka
for _, row in df.iterrows():
    message = row.to_dict()
    producer.send('stress-topic', value=message)
    print(f"Enviado: {message}")
    time.sleep(1)  # Enviar 1 mensaje por segundo

producer.flush()
producer.close()


# In[ ]:




