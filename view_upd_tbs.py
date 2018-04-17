from sigcatmin.settings import *
from sigcatmin.pyscmin import *
import re
import sys

conn = Connection().conn
cursor = conn.cursor()


# ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
# SG_D_SIMULTANEOS
def update_simultaneos(date):
    cursor.callproc(
        'DATA_CAT.PACK_DBA_SIMULTANEIDAD.P_INSERT_SG_D_SIMULTANEOS',
        [date]
    )


# ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
# SG_D_SIMULCOOR
def update_simulcoor(date):
    sys_refcur = cursor.var(oracle.NUMBER)
    cursor.callfunc(
        'DATA_CAT.PACK_DBA_SIMULTANEIDAD.F_CHECK_EXISTS_SIMULCOOR',
        sys_refcur,
        [date]
    )
    if not sys_refcur.getvalue():
        get_summary_pesicu(date)


def get_summary_pesicu(date):
    sys_refcur = cursor.var(oracle.CURSOR)
    cursor.callfunc(
        'DATA_CAT.PACK_DBA_SIMULTANEIDAD.F_GET_SUMMARY_PESICU',
        sys_refcur,
        [date]
    )
    datos = [[x[1], x[2], get_union_quads(x[-1], x[0], date)] for x in sys_refcur.getvalue()]
    for x in datos:
        for cc in x[-1]:
            insert_simulcoor(date, x[0], x[1], cc[0], cc[1][0], cc[1][1])


def get_union_quads(quads, zone, date):
    quads_sql = "'%s'" % quads.replace(', ', "', '")
    sys_refcur = cursor.var(oracle.CURSOR)
    cursor.callfunc(
        'DATA_CAT.PACK_DBA_SIMULTANEIDAD.F_GET_UNION_QUADS',
        sys_refcur,
        [quads_sql, zone, date]
    )
    return [set_coords(x) for x in sys_refcur.getvalue()][0]


def set_coords(coords):
    cc_tmp = ', '.join(re.findall("\d+\.\d+ \d+\.\d+", str(coords)))
    cc = [[i, map(lambda n: float(n), x.split(' '))] for i, x in enumerate(cc_tmp.split(', '))]
    cc = remove_nodes(cc)
    return cc


def insert_simulcoor(*args):
    cursor.callproc(
        'DATA_CAT.PACK_DBA_SIMULTANEIDAD.P_INSERT_SG_D_SIMULCOOR',
        args
    )


# ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
# SG_D_DMXGRSIMUL
def update_dmxgrsimul(date):
    cursor.callproc(
        'DATA_CAT.PACK_DBA_SIMULTANEIDAD.P_INSERT_SG_D_DMXGRSIMUL',
        [date]
    )


# ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
# SG_D_CARTAXDERESIMUL
def update_cartaxderesimul(date):
    cursor.callproc(
        'DATA_CAT.PACK_DBA_SIMULTANEIDAD.P_INSERT_SG_D_CARTAXDERESIMUL',
        [date]
    )


# ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
# SG_D_DEMAXDERESIMUL
def update_demaxderesimul(date):
    cursor.callproc(
        'DATA_CAT.PACK_DBA_SIMULTANEIDAD.P_INSERT_SG_D_DEMAXDERESIMUL',
        [date]
    )


def update_tables(date):
    try:
        update_simultaneos(date)
        update_simulcoor(date)
        update_dmxgrsimul(date)
        update_cartaxderesimul(date)
        update_demaxderesimul(date)
    except Exception as e:
        with open(os.path.join(os.path.dirname(__file__), 'log.txt'), 'w') as f:
            f.write(e.message)
            f.close()


if __name__ == "__main__":
    date = sys.argv[1]
    update_tables(date)
