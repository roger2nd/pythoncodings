import os
from os import listdir
from os.path import isfile, join
from os import walk
import pandas as pd
import numpy as np

mypath = 'dados/postos_pluviometricos_CE/slice'

onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]

def path_filter(filename=None, path=None):
        
        if type(path) == str: 
            files_path = listdir(os.getcwd()) #Lista todos os arquivos e diretorios locais (local de execucao do script)
            sa = path.split('/')
            for n in files_path: #Verifica se o path fornecido existe no local de execucao do script. (Pastas)
                if n in sa: 
                    sa = 0 
                    break #Sai do loop apos a primeira ocorrencia
            if bool(sa): raise TypeError('Caminho informado nao existe no diretorio atual')
        else: raise ValueError('Formato incorreto de string')        
        if type(filename) == str: #Asserta que o nome do arquivo seja um valor valido (string)
            files = [f for f in listdir(path) if isfile(join(path, f))] #Verifica se o arquivo existe no path fornecido (Arquivo)
            if filename in files: path = path + '/' + filename  #Monta o nome do caminho para o filename fornecido
            else: raise TypeError('Arquivo inexistente no caminho ou sem extensao')
        else: raise ValueError('Formato incorreto ' + str(filename) + '  ' + str(path) + ' deve ser uma string')
        return path


def funceme_pluviometria (filename=None, path=None, sep=';', verbose=False):
    
        path = path_filter(filename=filename, path=path) #Verifica se o arquivo fornecido existe e monta o path
        if verbose: print('Convertendo '+filename+'...')
        
        if type(sep) == str:
            separador = ['.', ':', ';', ',']
            if sep in separador:
                db = pd.read_csv(path, sep=sep)
            else: raise ValueError('Separador deve ser . , ; :  default = ";"')
        else: raise TypeError('sep deve ser string')
        
        Tamanho = db.shape[0]*31
        if Tamanho == 0:
            if verbose: print('Conteudo do '+filename+' nao possui dados')
            return 0, 0        
        
        Municipio = db.iloc[0,0]
        if ' ' in Municipio: Municipio = db.iloc[0,0].replace(' ','_') #Verifica e substitui espaços na string   
        Posto = db.iloc[0,1]
        if ' ' in Posto: Posto =  db.iloc[0,1].replace(' ', '_')
        posicao = [db.iloc[0,2], db.iloc[0,3]]
        col_name = Municipio + '_'+ Posto
        #Variaveis de Controle
        #inicio_ano = db.iloc[0,4]
        #fim_ano = db.iloc[len(db)-1, 4]
        #last_mes = db.iloc[len(db)-1, 5]
        #Tamanho = db.shape[0]*31
            
        #Formatando o dataframe
        db_dias = db.drop(['Municipios', 'Postos', 'Latitude', 'Longitude','Total'], axis=1)
        db_dias.index = db_dias.Anos
        db_dias = db_dias.drop(['Anos'], axis=1)
        
        #Construindo list dos "anos" a partir dos "Anos" do dataset
        anos = list()
        for n in db.Anos.drop_duplicates():
            anos.append(n)
        anos.sort()
        #Construindo list dos "meses" a partir dos "Meses" do dataset
        meses = list()
        for n in db.Meses.drop_duplicates():
            meses.append(n)
        meses.sort()
        #Listas para armazenamento da data individualizadas
        year = list()
        month = list()
        day = list()
        #Valor pluviometrico
        value = list()
        #Dicionario utlizado para construir a serie temporal -> pd.to_datetime
        dados = {'day':day, 'month':month, 'year':year, col_name: value}
        idi = 0 #Variavel de controle da posição dos indices do dataset
        #Bloco de contrucao das datas no formato colunas
        for a in anos:
            for m in meses:
                if idi >= len(db_dias): break
                if type(db_dias.loc[a, 'Meses'].tolist()) == float or type(db_dias.loc[a, 'Meses'].tolist()) == int: 
                    if int(db_dias.loc[a, 'Meses'].tolist()) == m:
                        for d in db_dias.columns:
                            if d != 'Meses': 
                                value.append(db_dias.iloc[idi, int(d.removeprefix('Dia'))])
                                year.append(a)
                                month.append(m)
                                day.append(int(d.removeprefix('Dia')))
                            else: pass
                        idi += 1
                    else: pass
                elif m in db_dias.loc[a, 'Meses'].tolist():#if sum(db_dias.loc[str(a), 'Meses'] == m): #Verifica se existe o mes "m" no ano "a"
                    for d in db_dias.columns:
                        if d != 'Meses': 
                            value.append(db_dias.iloc[idi, int(d.removeprefix('Dia'))])
                            year.append(a)
                            month.append(m)
                            day.append(int(d.removeprefix('Dia')))
                        else: pass
                    idi += 1
                else: pass
        #Criando novo dataframe com a nova forma dos dados        
        df = pd.DataFrame(dados)
        if len(df) != Tamanho: 
            print('O arquivo '+ filename + ' nao pode ser convertido corretamente')#Levanta erro e continua execuucao
            return 0, 0
        df.drop(df[df[col_name] == 888.0].index, inplace=True)
        df['Data'] = pd.to_datetime(dict(list(dados.items())[:3]), format='%d-%m-%Y', errors='coerce')
        if df.Data.isnull().sum() > 0: 
            print('O arquivo '+ filename + ' foi convertido com valores NaN')#Levanta erro e continua a execucao
            return 0, 0
        
        df.index = df.Data
        
        dataframe = df.drop(dict(list(dados.items())[:3]).keys(), axis=1)
        dataframe = dataframe.drop(['Data'], axis=1)
        dataframe[col_name] = dataframe[col_name].replace(999.0, 0)
                
        return dataframe, col_name

for n in onlyfiles:
    df, name =  funceme_pluviometria(filename=n, path=mypath, sep=';', verbose=True)
    if name != 0:
        n = n.removesuffix('.txt') 
        caminho = 'dados/postos_pluviometricos_CE/convertidos/pluviometria_'+ name +'_'+ n +'.csv'
        df.to_csv(caminho, index=True)