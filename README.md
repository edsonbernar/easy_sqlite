
Simples ORM Python para manipulação de base Sqlite em operações de CRUD, quando não há a necessidade de se incorporar ao deploy outros orm's complexos.

Dependências:

- sqlite3

Exemplos de uso

```
import sqlite3

from easy_sqlite import Base, Column, String, Char, Integer, Text, \
    Date, Numeric, Boolean, ForeignKey, Relation

class Produtos(Base):
    __table__ = "produtos"
    
    id = Column(Integer(10), primary_key=True, autoincrement=True)   
    descric = Column(String(30))   
    preco = Column(Numeric(10, 2), default=0)
    
dbconnection = sqlite3.connect('dt_base.db', check_same_thread=False)

prod = Produtos(dbconnection=dbconnection, autocommit=True))
prod.descric = 'COMPUTADOR' 
prod.preco = 100.00
prod.save()
```

Alteração
```
prod.load(id)
prod.preco = 50.00
prod.save()
```

Exclusão
```
prod.load(id)
prod.delete()
```

