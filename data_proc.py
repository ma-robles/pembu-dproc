# -*- coding: utf-8 -*-
# author: Miguel Angel Robles Roldan
# email: miguel.robles@atmosfera.unam.mx

''' Biblioteca de procesamiento de archivos de datos de PEMBU
'''

import datetime as dt
DATEFMT='%Y/%m/%d %H:%M:%S'

def count_nan(filename, nan_str='nan', ofilename=None, ndata=None, datefmt=DATEFMT):
    '''Cuenta datos no válidos
    
    filename: nombre del archivo de entrada
    nan_str: cadena que representa los datos no válidos
    ofilename: nombre del archivo de salida
    '''
    fdates= get_fdates(filename, datefmt=datefmt)
    fdate,data=next(fdates)
    if ndata==None:
        ndata=len(data)
    #init counters
    cnt_data, cnt_miss=[0]*ndata,[0]*ndata
    while(True):
        for i,d in enumerate(data):
            if d==nan_str:
                cnt_miss[i]+=1
            else:
                cnt_data[i]+=1
        try:
            fdate,data=next(fdates)
        except:
            break
    return cnt_miss, cnt_data

def get_dates(start, end, step, datefmt=DATEFMT ):
    '''crea fechas
    '''
    idate=start
    while idate<end:
        yield idate.strftime(datefmt)
        idate+=step
def get_fdates(filename,datefmt=DATEFMT ):
    '''obtiene fechas del archivo
    '''
    for line in open(filename):
        try:
            dt.datetime.strptime(line.split(',')[0], datefmt)
            yield line.split(',')[0], line[:-1].replace(' ','').split(',')[1:]
        except:
            pass

def fill( filename, start, end, step, ndata=None, rep_word='nan', datefmt=DATEFMT, ofilename=None):
    ''' rellena datos con rep_word y con un paso de tiempo dado por step
    
    filename: nombre de archivo de entrada
    start: fecha de inicio objeto datetime
    end: fecha de fin, objeto datetime
    step: paso de tiempo, objeto timedelta
    ndata: número de datos (columnas con datos), si no se especifica se detecta automaticamente
    rep_word: cadena con la que se representará los datos de relleno
    datefmt; formato de fecha en el archivo
    ofilename: Nombre del archivo de salida
    '''
    dates= get_dates(start, end, step)
    fdates= get_fdates(filename)
    fdate,data=next(fdates)
    if ndata==None:
        ndata=len(data)
    if ofilename==None:
        ofile=None
    else:
        ofile=open(ofilename, 'w')
    for date in dates:
        if date!=fdate:
            print(date, (','+rep_word)*ndata,sep='', file=ofile)
        else:
            print(date,','.join(data), sep=',', file=ofile)
            try:
                fdate,data=next(fdates)
            except:
                pass

if __name__=='__main__':
    a=dt.datetime.strptime('2019/01/01 00:00:00', DATEFMT)
    b=dt.datetime.strptime('2019/01/01 03:00:00', DATEFMT)
    c=dt.timedelta(minutes=15)
    fill('test.csv',a,b,c, rep_word='null', ofilename='ofile.csv')
