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

""" lista = [1,2,3,4,5,6,7,8,9,10]
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

# Intersection 
new_rdda = sc.parallelize([1,2,3,4])
new_rddb = sc.parallelize([3,4,5,6])
rddi = new_rdda.intersection(new_rddb)
rddi.collect()

# Subtract devuelve los datos de a menos lo de b
rdds = new_rdda.subtract(new_rddb)
rdds.collect()

# Cartesian producto cartesiano - operacion costosa
rddc = new_rdda.cartesian(new_rddb)
rddc.collect()

 """
rdd = sc.parallelize(range(1,6))
rdd.collect()

# Alternativa a collect
# take  
# takeWithSample
# top 
# takeOrdered

rdd.take(3)
rdd.takeSample(False, 3)
rdd.top(4)
rdd.takeOrdered(3) # Orden ascendente o unsando una key

# Reduce, Fold, Aggregate

rdd.reduce(lambda x, y: x + y)

from operator import add, sub, mul
rdd.reduce(add)
rdd.reduce(sub)
rdd.reduce(mul)

rdd.fold(0, add) # Fold crea el valor cero para la operacion en cada particion y luego al final otra vez
rdd.glom().collect()
rdd.fold(1, mul)












