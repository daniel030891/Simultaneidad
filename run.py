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
        # self.name = name
        self.cursor = conn.cursor()
        self.codigous, self.quads = list(), list()
        self.codes, self.rls = list(), dict()
        self.groups, self.subgroups = dict(), list()
        self.simul, self.coords = dict(), dict()
        self.zone = int()
        self.rows = dict()

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

        self.simul['Z%s' % self.zone] = {}

        sys_refcur = self.cursor.var(oracle.CURSOR)
        self.cursor.callfunc(
            'DATA_CAT.PACK_DBA_SIMULTANEIDAD.F_GET_RLS_CODIGOU_QUADS',
            sys_refcur,
            [sql, self.zone, self.date]
        )
        self.quads = [x for x in sys_refcur.getvalue()]

    def prepare_data(self):
        keys_all = list(set([x[0] for x in self.quads]))
        rls_tmp = {x: [i[1] for i in [n for n in self.quads if n[0] == x]] for x in keys_all}
        self.rls = {k: tuple(sorted(v)) for k, v in rls_tmp.items() if len(v) > 1}
        self.codes = [x for x in list(set([v for k, v in self.rls.items()]))]
        self.letters = list(string.ascii_uppercase)
        self.letters.extend([x + i for x in string.ascii_uppercase[:4] for i in self.letters])

    def get_groups(self):
        groups_tmp = [x for x in self.codes]
        for n in set([i for n in groups_tmp for i in n]):
            components = [x for x in groups_tmp if n in x]
            for i in components:
                groups_tmp.remove(i)
            groups_tmp += [list(set([i for n in components for i in n]))]
        for i, x in enumerate(groups_tmp, 1):
            self.simul['Z%s' % self.zone]['G%s' % i] = {"codigou": x}

    def get_subgroups(self):
        subgroups = [[list(x), [n for n in self.rls if self.rls[n] == x]] for x in self.codes]
        self.get_rows(subgroups)
        self.review_simult(subgroups)
        self.subgroups.sort()
        for k, v in self.simul['Z%s' % self.zone].items():
            n = int()
            subgrupos = dict()
            for i in self.subgroups:
                if i[0][0] in v['codigou']:
                    subgrupos[self.letters[n]] = {'derechos': i[0], 'hojas': i[1]}
                    n += 1
            self.simul['Z%s' % self.zone][k]['subgrupos'] = subgrupos

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
        self.res = json.dumps(self.simul, ensure_ascii=False).encode('utf8')
        # with open(os.path.join(TMP_FOLDER, 'asdasd.json'), 'w') as f:
        #     f.write(self.res)
        #     f.close()

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
    # name = 'daniel.json'
    poo = Simultaneidad(date)
    poo.main()
    stdout.write(poo.res)
    stdout.flush()
