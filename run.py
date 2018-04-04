# !/usr/bin/env python
# -*- coding: utf-8 -*-

import itertools
import string
import re
import sys
from sys import stdout

from nls import *
from sigcatmin.settings import *

conn = Connection().conn


class Simultaneidad(Messages):
    def __init__(self, date):
        super(self.__class__, self).__init__()
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

    def set_zone(self, value):
        self.zone = value

    def get_codigou(self):
        sys_refcursor = self.cursor.var(oracle.CURSOR)
        self.cursor.callfunc(
            'DATA_CAT.PACK_DBA_SIMULTANEIDAD.F_GET_CODIGOU_FROM_DATE',
            sys_refcursor,
            [self.date]
        )
        self.codigous = [[x[0], x[2]] for x in sys_refcursor.getvalue()]
        if not self.codigous:
            raise RuntimeError(self.not_registry)

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

    def get_subgroups(self):
        subgroups = [[list(x), [n for n in self.rls if self.rls[n] == x]] for x in self.codes]
        self.get_rows(subgroups)
        self.review_simult(subgroups)
        self.subgroups.sort()
        for k, v in self.simul[self.zone].items():
            n = int()
            for i in self.subgroups:
                if i[0][0] in v['codigou']:
                    p = [{'codigou': x, 'cuadricula': m, 'grupo': k, 'subgrupo': self.letters[enum], 'zona': self.zone,
                          'subgrupo_num': enum + 1} for enum, x in enumerate(i[0], n) for m in i[1]]
                    self.res.extend(p)
                    n += 1

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

    def review_simult(self, subgroups):
        for i in subgroups:
            sub = {x: self.rows[x] for x in i[1]}
            gp = self.analysis(sub)
            if gp:
                self.subgroups.extend([[i[0], x] for x in gp])
            else:
                self.subgroups.append(i)

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

    def export(self):
        import json
        self.res.sort(key=lambda x: [x["zona"], x["grupo"], x["subgrupo"], x["cuadricula"], x["codigou"]],
                      reverse=False)
        self.res = json.dumps(self.res, ensure_ascii=False).encode('utf8')

    def process(self, zone):
        self.set_zone(zone)
        self.get_quadrants()
        self.prepare_data()
        self.get_groups()
        self.get_subgroups()

    def main(self):
        self.get_codigou()
        self.process(17)
        self.process(18)
        self.process(19)
        self.export()


if __name__ == '__main__':
    date = sys.argv[1]
    # date = '03/01/2018'
    poo = Simultaneidad(date)
    poo.main()
    stdout.write(poo.res)
    stdout.flush()
