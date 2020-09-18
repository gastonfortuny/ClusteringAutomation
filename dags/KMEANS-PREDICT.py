from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow import DAG
from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import codecs
import os
import logging
import pymssql
from airflow.hooks.base_hook import BaseHook
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler
from scipy import stats
import datetime as dt
import psycopg2
import pickle


def load_datasets ():
        print('Inicia tarea load_dataset')
        #Cargo los datasets
        print('Inicia la carga de datasets (1/9)')
        olist_orders = pd.read_csv('./datasets/olist_orders_dataset.csv')
        print('(2/9)')
        olist_products = pd.read_csv('./datasets/olist_products_dataset.csv')
        print('(3/9)')
        olist_items = pd.read_csv('./datasets/olist_order_items_dataset.csv')
        print('(4/9)')
        olist_customers = pd.read_csv('./datasets/olist_customers_dataset.csv')
        print('(5/9)')
        olist_payments = pd.read_csv('./datasets/olist_order_payments_dataset.csv')
        print('(6/9)')
        olist_sellers = pd.read_csv('./datasets/olist_sellers_dataset.csv')
        print('(7/9)')
        olist_geolocation = pd.read_csv('./datasets/olist_geolocation_dataset.csv')
        print('(8/9)')
        olist_reviews = pd.read_csv('./datasets/olist_order_reviews_dataset.csv')
        print('(9/9)')
        olist_product_category_name = pd.read_csv('./datasets/product_category_name_translation.csv')
        print('Fin de Carga de datasets')

        #Uno los datasets
        print ('Inicia el marge de datasets (1/7)')
        all_data = olist_orders.merge(olist_items, on='order_id', how='left')
        print('(2/7)')
        all_data = all_data.merge(olist_payments, on='order_id', how='inner')
        print('(3/7)')
        all_data = all_data.merge(olist_reviews, on='order_id', how='inner')
        print('(4/7)')
        all_data = all_data.merge(olist_products, on='product_id', how='inner')
        print('(5/7)')
        all_data = all_data.merge(olist_customers, on='customer_id', how='inner')
        print('(6/7)')
        all_data = all_data.merge(olist_sellers, on='seller_id', how='inner')
        print('(7/7)')
        all_data = all_data.merge(olist_product_category_name,on='product_category_name',how='inner')
        print('Fin del marge') 

        #libero variables
        print('Limpio variables no utilizadas')
        del olist_orders
        del olist_products
        del olist_items
        del olist_customers
        del olist_payments
        del olist_sellers
        del olist_geolocation
        del olist_reviews
        del olist_product_category_name

        #Elimino columna review_comment_title y  review_comment_message(contienen comas)
        all_data= all_data.drop(['review_comment_title'], axis=1)
        all_data= all_data.drop(['review_comment_message'], axis=1)     

        #muestro cantidad de filas y columas de all_data
        print('La canitdad de finas y columnas es: ', all_data.shape)

        #guardo el nuevo dataset en disco
        all_data.to_csv('./datasets/all_data.csv', index = False)
        
        print('Fin de load_dataset')

        return all_data

def data_process():
        print('Inicia tarea data_process')
        #Cargo el dataset
        print('Inicia la carga de dataset ')
        all_data = pd.read_csv('./datasets/all_data.csv')

        #Muestro Cantidad de filas y columnas de all_data
        print('La canitdad de finas y columnas es: ', all_data.shape)
        print(all_data['order_purchase_timestamp'].head)

        # Cambio el tipo de datos en la columna de fecha 
        print('Cambiando tipo de datos para columnas fechas (1/8)')
        all_data['order_purchase_timestamp'] = pd.to_datetime(all_data['order_purchase_timestamp'], format='%Y-%m-%d %H:%M:%S')
        print('2/8')
        all_data['order_approved_at'] = pd.to_datetime(all_data['order_approved_at'], format='%Y-%m-%d %H:%M:%S')
        print('3/8')
        all_data['order_delivered_carrier_date'] = pd.to_datetime(all_data['order_delivered_carrier_date'], format='%Y-%m-%d %H:%M:%S')
        print('4/8')
        all_data['order_delivered_customer_date'] = pd.to_datetime(all_data['order_delivered_customer_date'], format='%Y-%m-%d %H:%M:%S')
        print('5/8')
        all_data['order_estimated_delivery_date'] = pd.to_datetime(all_data['order_estimated_delivery_date'], format='%Y-%m-%d %H:%M:%S')
        print('6/8')
        all_data['shipping_limit_date'] = pd.to_datetime(all_data['shipping_limit_date'], format='%Y-%m-%d %H:%M:%S')
        print('7/8')
        all_data['review_creation_date'] = pd.to_datetime(all_data['review_creation_date'], format='%Y-%m-%d %H:%M:%S')
        print('8/8')
        all_data['review_answer_timestamp'] = pd.to_datetime(all_data['review_answer_timestamp'], format='%Y-%m-%d %H:%M:%S')
        
        
        #Creo columnas mes_orden para la exploración de datos
        print('Creo columna Month_order')
        all_data['Month_order'] = all_data['order_purchase_timestamp'].dt.to_period('M').astype('str')

        #Muestro columna Month_order
        print('Muestro columna Month_order')
        all_data[['Month_order','order_purchase_timestamp']].head()

        # Elijo entradas que van desde 01-2017 hasta 08-2018
        # Porque hay datos que están fuera de balance con el promedio de cada mes en los datos antes del 01-2017 y después del 08-2018
        # basado en datos de compra / order_purchase_timestamp
        print('Restringiendo el dataset a compras entre el 01-2017 al 08-2018')
        start_date = "2017-01-01"
        end_date = "2018-08-31"

        after_start_date = all_data['order_purchase_timestamp'] >= start_date
        before_end_date = all_data['order_purchase_timestamp'] <= end_date
        between_two_dates = after_start_date & before_end_date
        all_data = all_data.loc[between_two_dates]
        print('La cantidad de filas y columas del dataset es de : ', all_data.shape)
        
        #Elimino variables no utilizadas
        del after_start_date
        del before_end_date
        del between_two_dates

        #Completo los datos nulos de order_approved_at con la media
        print('Completando datos nulos de columna order_approved_at') 
        missing_1 = all_data['order_approved_at'] - all_data['order_purchase_timestamp']
        add_1 = all_data[all_data['order_approved_at'].isnull()]['order_purchase_timestamp'] + missing_1.median()
        all_data['order_approved_at']= all_data['order_approved_at'].replace(np.nan, add_1)

        #Completo los datos nulos de order_delivered_carrier_date con la media 
        print('Completando datos nulos de columna order_delivered_carrier_date') 
        missing_2 = all_data['order_delivered_carrier_date'] - all_data['order_approved_at']
        add_2 = all_data[all_data['order_delivered_carrier_date'].isnull()]['order_approved_at'] + missing_2.median()
        all_data['order_delivered_carrier_date']= all_data['order_delivered_carrier_date'].replace(np.nan, add_2)

        #Completo los datos nulos de order_delivered_customer_date con la media 
        print('Completando datos nulos de columna order_delivered_customer_date') 
        missing_3 = all_data['order_delivered_customer_date'] - all_data['order_delivered_carrier_date']
        add_3 = all_data[all_data['order_delivered_customer_date'].isnull()]['order_delivered_carrier_date'] + missing_3.median()
        all_data['order_delivered_customer_date']= all_data['order_delivered_customer_date'].replace(np.nan, add_3)
        
        #Elimino filas con datos nulos
        print('Eliminando filas con datos nulos')
        all_data = all_data.dropna()

        #Transformo flotantes en enteros
        print('Transformando flotantes en enteros')
        all_data = all_data.astype({'order_item_id': 'int64', 
                        'product_name_lenght': 'int64',
                        'product_description_lenght':'int64', 
                        'product_photos_qty':'int64'})

        #Creo una columna order_process_time para ver cuánto tiempo llevará iniciar el pedido hasta
        # artículos son aceptados por los clientes
        print('Creando columna order_process_time')
        all_data['order_process_time'] = all_data['order_delivered_customer_date'] - all_data['order_purchase_timestamp']   

        #Creo una columna order_delivery_time para ver cuánto tiempo se requiere el tiempo de envío para cada pedido
        print('Creando columna order_delivery_time')
        all_data['order_delivery_time'] = all_data['order_delivered_customer_date'] - all_data['order_delivered_carrier_date'] 

        #Creo una columna order_time_accuracy para ver si desde el tiempo estimado hasta que algo sea apropiado o tarde
        # Si el valor es + positivo, entonces es más rápido hasta que, si es 0, está justo a tiempo, pero si es negativo, llega tarde
        print('Creando order_time_accuracy')
        all_data['order_accuracy_time'] = all_data['order_estimated_delivery_date'] - all_data['order_delivered_customer_date']                 

        #Creo una columna order_approved_time para ver cuánto tiempo tomará desde el pedido hasta la aprobación
        print('Creando order_approved_time')
        all_data['order_approved_time'] = all_data['order_approved_at'] - all_data['order_purchase_timestamp'] 

        #Creo una columna review_send_time para averiguar cuánto tiempo se envió la encuesta de satisfacción después de recibir el artículo.
        print('Creando review_send_time')
        all_data['review_send_time'] = all_data['review_creation_date'] - all_data['order_delivered_customer_date']

        #Cree una columna review_answer_time para averiguar cuánto tiempo llevará completar una revisión después de
        # envió una encuesta de satisfacción del cliente.
        print('Creando review_answer_time')
        all_data['review_answer_time'] = all_data['review_answer_timestamp'] - all_data['review_creation_date']

        # Combino las columnas product_length_cm, product_height_cm y product_width_cm para convertirlo en un volumen
        # con una nueva columna, volumen_producto
        print('Creando product_volume')
        all_data['product_volume'] = all_data['product_length_cm'] * all_data['product_height_cm'] * all_data['product_width_cm']

        #Guardo el nuevo dataset en disco
        all_data.to_csv('./datasets/all_data_process.csv', index = False)    
        print('Fin de data_process')

        return all_data

def data_scaler():
        print('Inicia tarea data_scaler')
        #Cargo el dataset
        print('Inicia la carga de dataset ')
        all_data = pd.read_csv('./datasets/all_data_process.csv')

        #Transformo order_purchase_timestamp a datatime
        print('Transformando order_purchase_timestamp a datatime')
        all_data['order_purchase_timestamp'] = pd.to_datetime(all_data['order_purchase_timestamp'], format='%Y-%m-%d %H:%M:%S')

        #Creo dataframe RFM
        print ('Creando dataframe RFM')
        pin_date=max(all_data.order_purchase_timestamp) + dt.timedelta(1)
        rfm = all_data.groupby('customer_unique_id').agg({
        'order_purchase_timestamp' : lambda x: (pin_date - x.max()).days,
        'order_item_id' : 'count', 
        'payment_value' : 'sum'})
        

        #Renombro columnas RFM
        print('Renombrando columnas RFM')
        rfm.rename(columns = {'order_purchase_timestamp' : 'Recency', 
                        'order_item_id' : 'Frequency', 
                        'payment_value' : 'Monetary'}, inplace = True)

        #Imprimo cabecera de RFM
        print(rfm.head())

        #Elimino outliers de columna Monetary
        print('Eliminando outliers de columna Monetary')
        outliers1_drop = rfm[(rfm['Monetary'] > 1500)].index
        rfm.drop(outliers1_drop, inplace=True)
        print(rfm.head())

        # Creo grupos de clientes basados en Recency, Frequency y Monetary
        #Porque Recency si cuantos menos días mejor, hará el pedido al revés
        print('Creando Grupos RFM')
        r_labels = range(3, 0, -1)
        r_groups = pd.qcut(rfm.Recency, q = 3, labels = r_labels).astype('int')

        # Debido a que la frecuencia está muy en el valor 1, entonces no puede usar qcut,
        #porque el valor se apoyará más
        f_groups = pd.qcut(rfm.Frequency.rank(method='first'), 3).astype('str')
        #rfm['F'] = np.where((rfm['Frequency'] != 1) & (rfm['Frequency'] != 2), 3, rfm.Frequency)

        m_labels = range(1, 4)
        m_groups = pd.qcut(rfm.Monetary, q = 3, labels = m_labels).astype('int')

        rfm['R'] = r_groups.values
        rfm['F'] = f_groups.values
        rfm['M'] = m_groups.values
        print(rfm.head())

        #Categorizo grupo F
        print('Categorizando grupo F')
        rfm['F'] = rfm['F'].replace({'(0.999, 30871.333]' : 1,
                        '(30871.333, 61741.667]' : 2,
                        '(61741.667, 92612.0]' : 3}).astype('int')
        print(rfm.head())                    
        
        #Creo columna Segment y Score
        print('Creando columnas Segment y Score')
        rfm['RFM_Segment'] = rfm.apply(lambda x: str(x['R']) + str(x['F']) + str(x['M']), axis = 1)
        rfm['RFM_Score'] = rfm[['R', 'F', 'M']].sum(axis = 1)        

        #Creo columna Labels
        print('Creando columnas Labels')
        score_labels = ['Bronze', 'Silver', 'Gold']
        score_groups = pd.qcut(rfm.RFM_Score, q=3, labels = score_labels)
        rfm['RFM_Level'] = score_groups.values        

        #Escalamos RFM
        print('Escalando RFM')
        rfm_log = rfm[['Recency', 'Monetary']].apply(np.log, axis = 1).round(3)
        rfm_log['Frequency'] = stats.boxcox(rfm['Frequency'])[0]
        scaler = StandardScaler()
        rfm_scaled = scaler.fit_transform(rfm_log)
        rfm_scaled = pd.DataFrame(rfm_scaled, index = rfm.index, columns = rfm_log.columns)
        

        #Guardo el nuevo dataset en disco
        rfm_scaled.to_csv('./datasets/rfm_scaled.csv', index = False)
        rfm.to_csv('./datasets/rfm.csv')

        print('Fin de data_scaler')
        return rfm_scaled     

def data_predict():
        print('Inicia tarea data_model')
        #Cargo los datasets
        print('Inicia la carga de los datasets ')
        rfm_scaled = pd.read_csv('./datasets/rfm_scaled.csv')
        rfm = pd.read_csv('./datasets/rfm.csv')
        all_data = pd.read_csv('./datasets/all_data_process.csv')
        print(rfm.head())

        #Cargo Modelo en disco
        with open('/usr/local/airflow/models/model.pkl','rb') as f:
                clus= pickle.load(f)

        #Realizo predicción
        print('leyo el archivo')
        clus.predict(rfm_scaled)        

        #Muestro categorias
        print(clus.labels_)

        #Asigno categorias a dataset rfm
        rfm['K_Cluster'] = clus.labels_
        print('Cabecera de dataset rfm con clusterización: ')
        print(rfm.head())

        #Asigno categorias al dataset all_data
        print('Uniendo dataset rfm con dataset original')   
        rfm_K=rfm[['customer_unique_id','K_Cluster']] 
        all_data_cluster = all_data.merge(rfm_K, on='customer_unique_id', how='inner')   

        #Guardo el nuevo dataset en disco
        all_data_cluster.to_csv('./datasets/all_data_cluster.csv', index = False)
        rfm.to_csv('./datasets/rfm_cluster.csv', index = False)


        print('Fin de data_model')
        return all_data_cluster 


def get_connection():
        c= BaseHook.get_connection('postgres_default') 
        return c


def execute_insert():
        #Creo Connexión con mssql
        print('Creo conexión a pgsql')
        c = get_connection()        
        conn_string = "host="+ c.host +" port="+ str(c.port) +" dbname="+ c.schema +" user=" + c.login  +" password="+ c.password
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()

        #Inserto en tabla Ventas    
        print('Insertando ventas dataframe')    
        sql = "INSERT INTO ventas VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"      
        df =  pd.read_csv('./datasets/all_data_cluster.csv')
        data = [tuple(x) for x in df.values]
        cursor.executemany(sql, data)

        #Inserto en tabla RFM 
        print('Insertando rfm dataframe')       
        sql = "INSERT INTO public.rfm VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"      
        df =  pd.read_csv('./datasets/rfm_cluster.csv')
        data = [tuple(x) for x in df.values]
        cursor.executemany(sql, data)

        #Commiteo
        conn.commit()

        #Cierro la conexión
        conn.close()
        return data         

default_dag_args = {
        'start_date': datetime(2020, 8, 1,8),
        'email': ['gastonfortuny@gmail.com'],
        'email_on_failure': True,
        'email_on_retry': True,
        'project_id' : 'Diplo',
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
}

with DAG('KMEANS-PREDICT',
        schedule_interval = '@daily',
        catchup = False,
        default_args=default_dag_args) as dag:

        t_start = DummyOperator(task_id='start')        
        t_load_datasets = PythonOperator(task_id='load_datasets', python_callable=load_datasets, dag=dag)
        t_data_process = PythonOperator(task_id='data_process', python_callable=data_process, dag=dag)    
        t_data_scaler = PythonOperator(task_id='data_scaler', python_callable=data_scaler, dag=dag) 
        t_data_predict = PythonOperator(task_id='data_predict', python_callable=data_predict, dag=dag) 
        t_execute_insert = PythonOperator(task_id='execute_insert', python_callable=execute_insert, dag=dag)
        t_end = DummyOperator(task_id='end')
        t_start >>  t_load_datasets >> t_data_process >> t_data_scaler >> t_data_predict  >> t_execute_insert  >> t_end    
