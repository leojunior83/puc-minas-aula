from pyspark.sql import SparkSession

if __name__ == "__main__":

        spark = SparkSession.builder.getOrCreate()

        #Ler Dados
        df = spark.read.csv("s3://bucket-aula-594398268477/raw-data/vgsales.csv",
            inferSchema=True, header=True, sep=',')
    
        df.createOrReplaceTempView('jogos')
    
        #Realizar consultas

        #Média de vendas global de jogos do PS3 em 2007
        Media_Vendas_2007 = spark.sql(""" 
                SELECT 
                Platform, Genre,
                    Round(AVG(Global_Sales), 3) as Media_Vendas_2007
                FROM jogos
                WHERE Platform ='PS3' AND Year = 2007
                GROUP BY Platform, Genre
                ORDER BY Platform
                LIMIT(10)
        """)

        #Média de vendas global de jogos do PS3 em 2010 
        Media_Vendas_2010 = spark.sql(""" 
                SELECT 
                Platform, Genre,
                        Round(AVG(Global_Sales), 3) as Venda_Media_2010
                FROM jogos
                WHERE Platform ='PS3' AND Year = 2010
                GROUP BY Platform, Genre
                ORDER BY Platform
                LIMIT(10)
        """)

        #Média de vendas global de jogos do PS3 em 2015
        Media_Vendas_2015 = spark.sql(""" 
                SELECT 
                Platform, Genre,
                        Round(AVG(Global_Sales), 3) as Venda_Media_2015
                FROM jogos
                WHERE Platform ='PS3' AND Year = 2015
                GROUP BY Platform, Genre
                ORDER BY Platform
                LIMIT(10)
        """)

        #Escrever os dados em Parquet
        Media_Vendas_2007.write.format('parquet').save("s3://bucket-aula-594398268477/process-data/media-vendas-2007")
        Media_Vendas_2010.write.format('parquet').save("s3://bucket-aula-594398268477/process-data/media-vendas-2010")
        Media_Vendas_2015.write.format('parquet').save("s3://bucket-aula-594398268477/process-data/media-vendas-2015")
        

        #Lendo os dados em Parquet.
        Media_Vendas_2007 = spark.read.parquet("s3://bucket-aula-594398268477/process-data/media-vendas-2007")
        Top_Vendas_2015 = spark.read.parquet("s3://bucket-aula-594398268477/process-data/media-vendas-2010")
        Top_Vendas_EU = spark.read.parquet("s3://bucket-aula-594398268477/process-data/media-vendas-2015")


        #Montando a tabela concatenada.
        tabela_final = (
                Media_Vendas_2007
                .join(Media_Vendas_2010, ['Platform', 'Genre'])
                .join(Media_Vendas_2015, ['Platform', 'Genre'])
        )
        
        #Salvar a tabela concatenada no S3.
        tabela_final.write.format('parquet').save("s3://bucket-aula-594398268477/process-data/tabela-final")
