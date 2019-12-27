
import yaml
import sys
import psycopg2
import re

cost_re = re.compile(r".*\s+\(cost=.*\.\.(?P<high_cost>.*) rows=.* width=.*\)");

class OsmDatabase:
    def __init__(self):
        # TODO get this from the project.mml
        self.conn = psycopg2.connect("dbname=gis")

    def create_explain_query(self, table):
        template_vars = {
            "scale_denominator": "30000",
            "pixel_width": "4",
            "pixel_height": "4",
            "bbox": "ST_SetSRID('BOX3D(853648.7318888485 5848349.908155404,863432.671509351 5858133.847775906)'::box3d, 3857)",
        }
        query = "EXPLAIN SELECT * FROM %s" % table
        if "!bbox!" not in table:
            query += " WHERE way && !bbox!"
        query += ";"        
        for needle, replacement in template_vars.items():
            query = query.replace("!%s!" % needle, replacement)
        return query

    def layer_cost(self, layer):
        query = self.create_explain_query(layer['Datasource']['table'])
        cur = self.conn.cursor()
        cur.execute(query)
        res = cur.fetchone()
        m = cost_re.match(res[0])
        cur.close()
        return float(m.group("high_cost"))

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if self.conn:
            self.conn.close()
        return False

def scan_mml_file(filename):
    with open(filename) as fp:
        project = yaml.safe_load(fp)
    
    layers = [l for l in project['Layer'] if l['Datasource']['type'] == 'postgis']
    with OsmDatabase() as db:
        layer_costs = {l['id']: db.layer_cost(l) for l in layers}

    print("Total cost", round(sum(layer_costs.values())))
    print("Most expensive", *max(layer_costs.items(), key=lambda c: c[1]))

if __name__ == "__main__":
    scan_mml_file(sys.argv[1])


