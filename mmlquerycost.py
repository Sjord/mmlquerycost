
import yaml
import sys
import psycopg2

def create_explain_query(table):
    template_vars = {
        "scale_denominator": "30000",
        "pixel_width": "4",
        "pixel_height": "4",
        "bbox": "ST_SetSRID('BOX3D(853648.7318888485 5848349.908155404,863432.671509351 5858133.847775906)'::box3d, 3857)",
    }
    for needle, replacement in template_vars.items():
        table = table.replace("!%s!" % needle, replacement)
    return "EXPLAIN SELECT * FROM %s;" % table

def layer_cost(layer):
    query = create_explain_query(layer['Datasource']['table'])
    # print(query)

    # TODO get this from the project.mml
    # TODO keep the same connection
    conn = psycopg2.connect("dbname=gis")
    cur = conn.cursor()
    cur.execute(query)
    res = cur.fetchone()
    cur.close()
    conn.close()
    print(res)

def scan_mml_file(filename):
    with open(filename) as fp:
        project = yaml.safe_load(fp)
    layers = [l for l in project['Layer'] if l['Datasource']['type'] == 'postgis']
    costs = [layer_cost(l) for l in layers]
    print(costs)

if __name__ == "__main__":
    scan_mml_file(sys.argv[1])


