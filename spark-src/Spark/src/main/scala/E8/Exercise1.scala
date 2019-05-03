package main.scala.E8

class Exercise1 {
//SQOOP TABLE IMPORT (write in a signle line)

  /*
    sqoop import
      --connect "jdbc:oracle:thin:@137.204.78.85:1521:SISINF"
      --username amordenti --password amordenti
      --target-dir /user/mgondolini/sqoop_ex
      --query 'select * from ( select * from amordenti.ppe) where \$CONDITIONS’
      --hive-import
      --hive-database gruppo5fila2
      --hive-table PPE
      --split-by zipcode
      --as-parquetfile
  */

}
