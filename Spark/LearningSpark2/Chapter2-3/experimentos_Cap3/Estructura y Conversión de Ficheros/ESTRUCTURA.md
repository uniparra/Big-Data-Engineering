<h4>En los DataFrame de spark, existen dos objetos que se pueden extraer desde un dataframe</h4>
  1. Objeto Column. Todos los registros de un column tienen el mismo tipo de dato. Se pueden listar todas las columnas apelando a su nombre y hacer operaciones en los valores usando expresiones computacionales o relacionales (expr("'nombreCol'Rx") o col("'nombreCol'") 
  
  2. Objeto Row. No todos los registros de una fila tienen por qué ser  del mismo tipo.
