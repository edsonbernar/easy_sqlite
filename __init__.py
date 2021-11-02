# -*- coding: utf-8 -*-
#
# easy_sqlite - Python libraries to deal with Sqlite module
#
# Copyright (C) 2016-2021
# Copyright (C) Edson Bernardino <edsones at yahoo.com.br>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Library General Public License as
# published by the Free Software Foundation, either version 2.1 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library General Public License for more details.
#
# You should have received a copy of the GNU Library General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# easy_sqlite - Módulo Python para inteface com base Sqlite

#
# Copyright (C) 2016-2021
# Copyright (C) Edson Bernardino <edsones arroba yahoo.com.br>
#
# Este programa é um software livre: você pode redistribuir e/ou modificar
# este programa sob os termos da licença GNU Library General Public License,
# publicada pela Free Software Foundation, em sua versão 2.1 ou, de acordo
# com sua opção, qualquer versão posterior.
#
# Este programa é distribuido na esperança de que venha a ser útil,
# porém SEM QUAISQUER GARANTIAS, nem mesmo a garantia implícita de
# COMERCIABILIDADE ou ADEQUAÇÃO A UMA FINALIDADE ESPECÍFICA. Veja a
# GNU Library General Public License para mais detalhes.
#
# Você deve ter recebido uma cópia da GNU Library General Public License
# juntamente com este programa. Caso esse não seja o caso, acesse:
# <http://www.gnu.org/licenses/>
#

from datetime import date, datetime

class instrumentedlist(list):
    def __init__(self, parent=None, *args):
        list.__init__(self, *args)
        self.__parent__ = parent 

    def pop(self, nEle):
        ite = None
        try:
            ite = super(instrumentedlist, self).pop(nEle)
            if self.__parent__:
               ite.__state__ = 'update'
               self.__parent__.__deletes_rows__.append(ite)
        except:    
            raise

class ForeignKey(object):
    def __init__(self, cKey):
        pass
      
class Relation(object):
    def __init__(self, cClass, foreignKey= '', backref='', 
                 cascade="all, delete, delete-orphan", passive_updates=False):
      
        self.__parent__ = None
        self.__parent__list__ = []
        self.foreignKey = foreignKey      
        self.cClass = cClass
        self.backref = backref
        self.cascade = cascade
        self.passive_updates = passive_updates
        self.__deletes_rows__ = [] 

    def load_Relationship(self):
        # Se houver conteúdo no campo primaryKey do parent     
        cPK = self.__parent__.__primary_key__
        parent_dbColumn = self.__parent__.__fields__[cPK]                             
        value_pk = getattr(self.__parent__, cPK)
        
        if value_pk:
            #oRows = eval(self.cClass + '()')
            oRows = self.cClass(dbconnection=self.__parent__.dbconnection)                          
            oRows.load(None, cWhere='%s = %s' % (self.foreignKey, 
                                    parent_dbColumn.Type.toString(value_pk)))
         
            if oRows.__state__ == 'add':            
                list_ = instrumentedlist(parent=self) 
            else:
                if oRows.__rows__:
                    list_ = oRows.__rows__
                else:
                    list_ = instrumentedlist(parent=self)
                    list_.append(oRows)
                            
            setattr(self.__parent__, self.__parent__list__, list_)
            oRows.__rows__.__parent__ = self
                        
            '''
            if oRows.__rows__:                
                setattr(self.__parent__, self.__parent__list__, oRows.__rows__)
                oRows.__rows__.__parent__ = self
            else:                
                list_ = instrumentedlist(parent=self) 
                list_.append(oRows) # carrega orm vazio para o list do pai                
            '''    
        else:
            setattr(self.__parent__, self.__parent__list__, 
                                    instrumentedlist(parent=self))             

        return 
                                                                         

class Char(object):
    def __init__(self, nTam):
        self.column = None
        self.Type = str
        self.toString = lambda vValue: "'%s'" % (vValue.replace("'", "''")) \
                                                if vValue != None else 'Null'  
        self.dbtoOrm = lambda vValue: vValue 
        self.length = nTam

class String(object):
    def __init__(self, nTam):
        self.column = None
        self.Type = str
        self.toString = lambda vValue: "'%s'" % (vValue.replace("'", "''")) \
                                                if vValue != None else 'Null'  
        self.dbtoOrm = lambda vValue: vValue 
        self.length = nTam
      
class Numeric(object):
    def __init__(self, nTam, nDec):
        self.column = None
        self.Type = float
        self.length = nTam
        self.decimal = nDec
        self.toString = lambda vValue: str(vValue) if vValue != None else 'Null'
        self.dbtoOrm = lambda vValue: vValue
                                                                                                                   
class Integer(object):
    def __init__(self, nTam):
        self.column = None
        self.Type = int
        self.length = nTam
        self.toString = lambda vValue: str(vValue) if vValue != None else 'Null'
        self.dbtoOrm = lambda vValue: vValue
   
class Text(object):
    def __init__(self):
        self.column = None
        self.Type = str
        self.toString = lambda vValue: "'%s'" % (vValue.replace("'", "''")) \
                                                if vValue != None else 'Null'  
        self.dbtoOrm = lambda vValue: vValue
        self.length = 400                                       

class Date(object):
    def __init__(self):
      
        self.column = None
        self.Type = date
        self.toString = lambda vValue: "'%s'" % (vValue.strftime('%Y-%m-%d')) \
                                                        if vValue else 'Null'
        self.dbtoOrm = lambda vValue: datetime.strptime(vValue, '%Y-%m-%d') \
                                                            if vValue else None
        self.length = 10
        #cValue = cValue.strftime("%Y-%m-%d")

class Boolean(object):
    def __init__(self):
        self.column = None
        self.Type = bool
        self.toString = lambda vValue: '1' if vValue else '0'
        self.dbtoOrm = lambda vValue: vValue                                      
       
class Column(object):
    def __init__(self, oType, ForeignKey=None, primary_key=False, default=None, 
                 autoincrement=False):
        self.Type = oType
        self.default = default
        self.primary_key = primary_key
        self.Type.column = self
        self.autoincrement = autoincrement             
      
class Base(object):
        
    __table__ = ''
    
    def __init__(self, table='', dbconnection=None, autocommit=False):
        
        self.dbconnection = dbconnection
        if self.dbconnection:
            self.cursor = self.dbconnection.cursor()
        
        if table:
            self.__table__ = table    
        
        self.autocommit = autocommit

        self.__fields__ = {}
        self.__Relationship__ = {}
        self.__primary_key__ = None
        self.__state__ = 'add'
        self.__rows__ = instrumentedlist() #[]
      
        for index, value in self.__class__.__dict__.items():
         
            if isinstance(value, Column):
                self.__fields__[index] = value
                setattr(self, index, value.default)                 
                if self.__fields__[index].primary_key:
                    self.__primary_key__ = index
         
            elif isinstance(value, Relation):
                self.__Relationship__[index] = value            
                self.__Relationship__[index].__parent__ = self
                self.__Relationship__[index].__parent__list__ = index
                setattr(self, index, instrumentedlist(
                                        parent=self.__Relationship__[index]))
                  
    def set_dbconnection(self, dbconnection=None):
        self.dbconnection = dbconnection
        self.cursor = self.dbconnection.cursor()                            

    def load(self, value_pk, cWhere=None): # carga pelo parâmetro
        
        cQuery = 'select '
        for key in self.__fields__.keys():
            cQuery += '%s, ' % (key)
        cQuery = '%s from %s' % (cQuery[:-2], self.__table__)
      
        # Se informado valor para primary_key
        if value_pk:
            cQuery += ' where %s = %s' % (self.__primary_key__, 
                self.__fields__[self.__primary_key__].Type.toString(value_pk))
        else:
            cQuery += ' where %s' % (cWhere)
      
        oResult = self.cursor.execute(cQuery).fetchall()        

        if len(oResult) == 1:
            for row in oResult:
                for field, vValue in row.items():
                    setattr(self, field, self.__fields__[field].Type.dbtoOrm(
                                                                    row[field]))                         

        elif len(oResult) > 1:                
            for nId, row in enumerate(oResult):
                self.__rows__.append(self.__class__(
                                                dbconnection=self.dbconnection))                   
                for field, value in row.items():
                    setattr(self.__rows__[nId], field, 
                            self.__fields__[field].Type.dbtoOrm(row[field]))            
                
        # Se não encontrar registros no load inicializa variávies c/ vlr default
        if len(oResult) > 0:
            self.__state__ = 'update'
        else:
            self.refresh()
         
        for relation in self.__Relationship__.values():         
            relation.load_Relationship()
        
    def refresh(self):
        self.__state__ = 'add'
        for index, value in self.__fields__.items():
            setattr(self, index, value.default)

        for relation in self.__Relationship__.values():              
            # list em parent que armazena instancias foreignkeys
            setattr(self, relation.__parent__list__,
                    instrumentedlist(parent=relation))                 
                                 
    def save(self): # Insert ou update
      
        value_pk = getattr(self, self.__primary_key__)
      
        if not self.__fields__[self.__primary_key__].autoincrement and \
            not value_pk:
            raise NameError(u'Value Primary Key is not defined!')            
        else:                                          
         
            if self.__state__ == 'add':
            
                cQuery = 'INSERT INTO %s (' % (self.__table__)
                cQuery_ = '('
                for cField, vValue in self.__fields__.items():
                    if not vValue.autoincrement:
                        cQuery  += cField + ','                        
                        cQuery_ += '%s,' % (vValue.Type.toString(
                                                        getattr(self, cField)))               
            
                cQuery = '%s) values %s)' % (cQuery[:-1], cQuery_[:-1])
                oResult = self.cursor.execute(cQuery)

                # Se autoincreneto, busca valor atribuído pelo sqlite
                if self.__fields__[self.__primary_key__].autoincrement:                    
                    oResult = self.cursor.execute(("SELECT "
                                        "last_insert_rowid() id")).fetchall()
                    
                    setattr(self, self.__primary_key__, oResult[0]['id'])
                    value_pk = oResult[0]['id'] 
                   
                ''' Propaga valor foreignKey e cria instruções inserts into, 
                para cada list existente no parent - Os lists são acessados 
                como literal em __Relationship__'''
                
                for relation in self.__Relationship__.values():              
                    # list em parent que armazena instancias foreignkeys
                    list_child = getattr(self, relation.__parent__list__)
                    
                    if list_child:
                        cQuery = 'INSERT INTO %s (' % (list_child[0].__table__)
                        cQuery_ = '('
                        for cField, vValue in list_child[0].__fields__.items():
                            if not vValue.autoincrement:
                                cQuery  += '%s,' %(cField)                                     
                                cQuery_ += '?,' 
                  
                        cQuery = '%s) values %s)' % (cQuery[:-1], cQuery_[:-1]) 
                  
                        list_recnos = []
                        for row in list_child:
                            setattr(row, relation.foreignKey, value_pk)                     
                     
                            list_ = []                     
                            for cField, vValue in row.__fields__.items():
                                if not vValue.autoincrement:                                                                     
                                
                                    list_.append(getattr(row, cField))
                                    # vValue.Type.toString(getattr(row, 
                                    #                        cField)) + ','                  

                            list_recnos.append(list_)
                  
                        oResult = self.cursor.executemany(cQuery, list_recnos) 
                
                if self.autocommit:  
                    self.dbconnection.commit()
                
                self.__state__ == 'update'
      
            elif self.__state__ == 'update':

                cQuery = 'UPDATE %s SET ' % (self.__table__)
                for cField, vValue in self.__fields__.items():
                    if cField != self.__primary_key__:
                        cQuery += '%s=%s,' % (cField, vValue.Type.toString(
                                                        getattr(self, cField)))               
            
                cQuery = "%s WHERE %s = %s" % (cQuery[:-1], 
                    self.__primary_key__,                                                  
                    self.__fields__[self.__primary_key__].Type.toString(
                                                                    value_pk))
                oResult = self.cursor.execute(cQuery)
                
                for relation in self.__Relationship__.values():              
                    
                    # list em parent que armazena instancias registros filhos 
                    list_child = getattr(self, relation.__parent__list__)    
                    
                    # Del registros marcados na alteração
                    for oChild in relation.__deletes_rows__:
                        oChild.delete()
                    
                    if list_child:
                        # Monta query para inserts
                        cSQL_IN = 'INSERT INTO %s (' % (list_child[0].__table__)
                        cQuery_ = '('
                        for cField, vValue in list_child[0].__fields__.items():
                            if not vValue.autoincrement:
                                cSQL_IN  += '%s,' %(cField)                                     
                                cQuery_ += '?,' 
                  
                        cSQL_IN = '%s) values %s)' % (cSQL_IN[:-1], 
                                                                cQuery_[:-1])                     
                        recnos_in = []
                        
                        # Monta query para updates         
                        cQuery = 'update %s set ' % (list_child[0].__table__)
                        for cField, vValue in list_child[0].__fields__.items():
                            if cField != list_child[0].__primary_key__:
                                cQuery += '%s=?,' % (cField)                                      

                        cQuery = "%s where %s=?" % (cQuery[:-1], 
                                                list_child[0].__primary_key__)                   
                        list_recnos = []

                        # Itera nos itens
                        for row in list_child:
                            list_ = []
                            
                            # Se não existir __primary_key__, insert
                            if not getattr(row, row.__primary_key__): 
                                setattr(row, relation.foreignKey, value_pk)

                                for cField, vValue in row.__fields__.items():
                                    if not vValue.autoincrement:                                                                                                    
                                        list_.append(getattr(row, cField))

                                recnos_in.append(list_)
                            
                            else: # Update
                                for cField, vValue in row.__fields__.items():                                                                     
                        
                                    if cField != row.__primary_key__:
                                        list_.append(getattr(row, cField))                     
                     
                                list_.append(getattr(row, row.__primary_key__))                     
                                list_recnos.append(list_)
                            
                        if recnos_in:                            
                            oResult = self.cursor.executemany(cSQL_IN, 
                                                              recnos_in)                         
                     
                        if list_recnos:                         
                            oResult = self.cursor.executemany(cQuery, 
                                                              list_recnos)                  
            
                if self.autocommit:
                    self.dbconnection.commit()
            
            elif self.__state__ == 'delete':
                raise NameError(u'Record deleted!') 
         
    def delete(self):
        ''' Com cursor.execute("PRAGMA foreign_keys = ON"), 
            delete cascade funciona para registros filhos ''' 
        value_pk = getattr(self, self.__primary_key__)
        if self.__state__ == 'update' and value_pk:
                                 
            cQuery = 'delete from %s where %s = %s' % (
                self.__table__, 
                self.__primary_key__, 
                self.__fields__[self.__primary_key__].Type.toString(value_pk))
            
            oResult = self.cursor.execute(cQuery)
            
            if self.autocommit:
                self.dbconnection.commit()

            self.__state__ == 'delete'
        #else:
        #    raise NameError(u'The record is not in edit mode!')
        