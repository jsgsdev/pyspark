import re
from pyspark import SparkContext
sc = SparkContext()

""" # RDD
textFile = sc.textFile('ejemplo.txt')

# Acciones
textFile.collect() # Hace una lista
textFile.count() # Cuenta la coleccion
textFile.first() # Saca la primera linea

# Transformacion 
segunda = textFile.filter(lambda linea: 'segunda' in linea)
segunda.collect() """

lista = [1,2,3,4,5,6,7,8,9,10]
rdd = sc.parallelize(lista)
rdd.collect()

filtrado_rdd = rdd.filter(lambda x: x > 1)
filtrado_rdd.collect()

# Map aplica una funcion a los elementes de un RDD
def suma(x):
    return x + 1 

filtrado_sumado_rrd = filtrado_rdd.map(suma)
filtrado_sumado_rrd.collect()

cuadrado_rdd = (filtrado_rdd
                .map(suma)
                .map(lambda x: (x,x**2)))
cuadrado_rdd.collect()

# flatMap es igual que map pero convierte el resultado en una lista simple

cuadrado_rdd = (filtrado_rdd
                .map(suma)
                .flatMap(lambda x: (x,x**2)))
cuadrado_rdd.collect()

# Sample
cuadrado_rdd.sample(False, 2).collect()
cuadrado_rdd.sample(True, 3).collect()

# distinct devuelve un nuevo rdd quitando duplicados 
lista_nueva = [1,1,2,2,3,3,4,4]
rrd_new = sc.parallelize(lista_nueva)
rrd_new.collect()
rrd_new.distinct().collect()

# groupBy devuelve un RDD con datos agrupados en formato de clave y valor
agrupado_rdd = rrd_new.groupBy(lambda x: x > 1)
agrupado_rdd.collect()

print( [ (x,sorted(y)) for (x,y) in agrupado_rdd.collect() ] )

# Transformaciones sobre dos RDD
rdda = sc.parallelize([1,2,3])
rddb = sc.parallelize([4,5,6])
rddu = rdda.union(rddb)
rddu.collect()