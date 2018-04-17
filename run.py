# !/usr/bin/env python
# -*- coding: utf-8 -*-

# Autor: Daniel Aguado H
# Fecha: 04/04/2018
# Ingemmet - Lima - Peru

# Importacion de librerias necesarias
import itertools  # Modulo para generar combinaciones y permutaciones
import string  # Modulo para el tratamiento de strings
import json
import uuid
import re  # Modulo que proporciona expresiones regulares
import sys  # Modulo que proporciona accesos a varios objetos del interprete python
from sys import stdout  # Archivo de salida estandard
from nls import *  # Mensajes utilizados en el procesamiento
# from sigcatmin.settings import *  # Configuracion necesaria para acceso a propiedades del desarrollo actual
from config import *

# Se realiza la conexion a base de datos
conn = Connection().conn

# Importando mensajes
msg = Messages()


# La simultaneidad por lo general se presenta cuando se realiza la publicacion de áreas declaradas
# extinguidas o de libre denunciabilidad, provocando la formulacion masiva de petitorios mineros sobre dichas areas.

# Simultaneidad total por fecha de libre denunciabilidad
class Simultaneidad(object):
    def __init__(self, date=None):
        self.date = date
        self.cursor = conn.cursor()
        self.codigous, self.quads = list(), list()
        self.codes, self.rls = list(), dict()
        self.groups, self.subgroups = dict(), list()
        self.simul, self.coords = dict(), dict()
        self.res = list()
        self.zone = int()
        self.rows = dict()
        self.num_group = 1
        self.id = uuid.uuid4().__str__().replace('-', '')[:17]

    # COnfigurar la fecha
    def set_date(self, value):
        self.date = value

    # Configurar la zona geografica
    def set_zone(self, value):
        self.zone = value

    # Identifica si la informacion ya fue procesada y almacenada en la
    # base de datos
    def check_date_exists_in_database(self):
        self.togle = self.cursor.var(oracle.NUMBER)
        self.cursor.callfunc(
            'DATA_CAT.PACK_DBA_SIMULTANEIDAD.F_CHECK_DATE_EXIST',
            self.togle,
            [self.date]
        )

    # Obtener los codigou de los derechos por fecha
    def get_codigou(self):
        sys_refcursor = self.cursor.var(oracle.CURSOR)
        self.cursor.callfunc(
            'DATA_CAT.PACK_DBA_SIMULTANEIDAD.F_GET_CODIGOU_FROM_DATE',
            sys_refcursor,
            [self.date]
        )
        self.codigous = [[x[0], x[2]] for x in sys_refcursor.getvalue()]
        if not self.codigous:
            raise RuntimeError(msg.not_registry)

    # Obteniendo cuadriculas por cada derecho minero
    def get_quadrants(self):
        zn = [x for x in self.codigous if x[1] == str(self.zone)]
        sql = "'%s'" % "', '".join([x[0] for x in zn])

        self.simul[self.zone] = {}

        sys_refcur = self.cursor.var(oracle.CURSOR)
        self.cursor.callfunc(
            'DATA_CAT.PACK_DBA_SIMULTANEIDAD.F_GET_RLS_CODIGOU_QUADS',
            sys_refcur,
            [sql, self.zone, self.date]
        )
        # Obtiene cada cuadricula que se intersecta con un derecho
        self.quads = [x for x in sys_refcur.getvalue()]

    # Tratamiento de informacion antes de ser procesada
    def prepare_data(self):
        # Obtiene la relacion de cuadriculas unicas
        keys_all = list(set([x[0] for x in self.quads]))  # Ejemplo: [u'15-F_1137', u'15-F_1138', u'15-F_1139']

        # Obtiene la relacion de cuadriculas con todos los drechos traslapados entre si
        rls_tmp = {x: [i[1] for i in [n for n in self.quads if n[0] == x]] for x in
                   keys_all}  # Ejemplo: {u'14-E_1455': (u'010009318', u'010016318')}

        # Obtiene aquellas cuadriculas que se intersectan con mas de un derecho
        self.rls = {k: tuple(sorted(v)) for k, v in rls_tmp.items() if
                    len(v) > 1}

        # Obtiene las agrupaciones unicas de derechos en 'self.rls'
        self.codes = list(set(
            [v for k, v in self.rls.items()]))  # Ejemplo: [(u'010009218', u'010016318'), (u'010009318', u'010016218')]

        self.letters = list(string.ascii_uppercase)
        self.letters.extend([x + i for x in string.ascii_uppercase[:4] for i in self.letters])

    # Obteniendo los grupos
    # Todos aquellos derechos que se intersectan entre si manteniendo una conectividad (sin espacios
    # vacios entre ellos) conforman un grupo.
    def get_groups(self):
        # Realiza un copia de 'self.codes' para no alterar su informacion en el procesamiento
        groups_tmp = self.codes[:]
        # Se realiza la iteracion de todos los valores unicos de derechos
        for n in set([i for n in groups_tmp for i in n]):
            # Se realiza un slicing de todas las grupaciones que contienen un derecho
            components = [x for x in groups_tmp if n in x]
            # Se iteran estas agrupaciones
            for i in components:
                # Se remueven las agrupaciones de la lista principal
                groups_tmp.remove(i)
            # Se agrega a la lista principal todos los valores unicos de la lista 'componentes'
            groups_tmp += [list(set([i for n in components for i in n]))]

        # self.num_group += 1
        for i, x in enumerate(groups_tmp, self.num_group):
            self.num_group += 1
            self.simul[self.zone][i] = {"codigou": x}

    # Obteniendo los subgrupos
    # Los subgrupos se forman a partir de la intersección entre derechos, los cuales confluyen en cuadriculas
    # Si dos o mas derechos se intersectan en hojas que no son adyacentes, o que se intersectan en un solo vertice,
    # estos deben ser separados conformando nuevos subgrupos.
    def get_subgroups(self, function):
        subgroups = [[list(x), [n for n in self.rls if self.rls[n] == x]] for x in self.codes]
        self.get_rows(subgroups)
        self.review_simult(subgroups)
        self.subgroups.sort()
        for k, v in self.simul[self.zone].items():
            n = int()
            for i in self.subgroups:
                if i[0][0] in v['codigou']:
                    [function(m, self.id, x, int(k), n + 1, self.date, str(self.zone), self.letters[n]) for x
                     in i[1] for m in i[0]]
                    n += 1

    # Obteniendo informacion de la base de datos como las coordenadas de cuadriculas
    def get_rows(self, subgroups):
        quads = [i for n in subgroups for i in n[1]]
        sql = "'%s'" % "', '".join(quads)
        clob = self.cursor.var(oracle.CLOB)
        clob.setvalue(0, sql)
        sys_refcursor = self.cursor.var(oracle.CURSOR)
        self.cursor.callfunc(
            'DATA_CAT.PACK_DBA_SIMULTANEIDAD.F_GET_COORDS_QUADS',
            sys_refcursor,
            [clob, self.zone, self.date]
        )

        self.rows = {
            x[0]: map(lambda m: (float(m[0]), float(m[-1])), [re.findall("\d+\.\d+", i) for i in x[1].split(',')])
            for x in sys_refcursor.getvalue()}

        self.rows = {k: [tuple(sorted(v[i: i + 2])) for i in range(len(v)) if i <= 3] for k, v in self.rows.items()}

    # Revision de cuadriculas simultaneas
    def review_simult(self, subgroups):
        for i in subgroups:
            sub = {x: self.rows[x] for x in i[1]}
            gp = self.analysis(sub)
            if gp:
                self.subgroups.extend([[i[0], x] for x in gp])
            else:
                self.subgroups.append(i)

    # Metodo encargado de identificar aquellos subgrupos conformados por cuadriculas no adyancentes o
    # cuadriculas que se intersectan en un solo vertice
    def analysis(self, subgroups):
        gp = list()
        coords_for_quads = [v for k, v in subgroups.items()]
        coords_summary = set(itertools.chain.from_iterable(coords_for_quads))

        for each in coords_summary:
            components = [x for x in coords_for_quads if each in x]
            for i in components:
                coords_for_quads.remove(i)
            coords_for_quads += [list(set(itertools.chain.from_iterable(components)))]
        if len(coords_for_quads) > 1:
            for n in coords_for_quads:
                a = [k for k, v in subgroups.items() for i in n if i in v]
                a = list(set(a))
                gp.append(a)
        return gp

    # Inserta la informacion procesada en la base de datos
    def insert_data_to_database(self, *args):
        self.cursor.callproc(
            'DATA_CAT.PACK_DBA_SIMULTANEIDAD.P_INSERT_ROWS_SIMULTANEIDAD',
            args
        )

    # Procesamiento de la informacion por zona geografica
    def process(self, zone):
        self.set_zone(zone)
        self.get_quadrants()
        self.prepare_data()
        self.get_groups()
        self.get_subgroups(self.insert_data_to_database)

    # Dispara el proceso de actualizacion de tablas en segundo plano
    def update_tables(self):
        import subprocess
        date = self.date.split('/')
        date = '%s%s%s' % (date[-1], date[1], date[0])
        subprocess.Popen('%s %s' % (UPDATE_TABLES, date), shell=True)

    # Metodo principal que ejecuta el algoritmo en su totalidad
    def main(self):
        try:
            self.check_date_exists_in_database()
            if not self.togle.getvalue():
                self.get_codigou()
                self.process(17)
                self.process(18)
                self.process(19)
            self.update_tables()
            self.res = json.dumps([{"state": 1, "msg": "Success"}])
        except Exception as e:
            self.res = json.dumps([{"state": 0, "msg": e.message}])


# Simultaneidad parcial segun derechos mineros consultados
class SimultaneidadEval(Simultaneidad):
    def __init__(self, codigous, zone, datum):
        super(self.__class__, self).__init__()
        self.zone = int(zone)
        self.codigous = [[x, str(self.zone)] for x in codigous.split('_')]
        self.datum = datum.lower()

    def get_date(self):
        if self.datum == 'psad-56':
            self.set_date('03/01/2016')
        elif self.datum == 'wgs-84':
            self.set_date('03/01/2018')

    def insert_data_to_database(self, *args):
        self.cursor.callproc(
            'DATA_CAT.PACK_DBA_SIMULTANEIDAD.P_INSERT_ROWS_SIMULTANEID_EVAL',
            args
        )

    # Metodo principal que ejecuta el algoritmo en su totalidad
    def main(self):
        try:
            self.get_date()
            self.process(self.zone)
            self.res = json.dumps([{"state": 1, "msg": "Success", "id": self.id}])
        except Exception as e:
            self.res = json.dumps([{"state": 0, "msg": e.message}])


# Si se ejecuta este fichero
if __name__ == '__main__':
    date = sys.argv[1]
    codigous = sys.argv[2]
    zone = sys.argv[3]
    datum = sys.argv[4]
    if date != "#":
        poo = Simultaneidad(date)
    else:
        poo = SimultaneidadEval(codigous, zone, datum)
    poo.main()
    # Devuelve el resultado en consola
    stdout.write(poo.res)
    stdout.flush()