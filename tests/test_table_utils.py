import json
import subprocess
import textwrap
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def run_node(script: str):
    result = subprocess.run(
        ["node", "-e", script],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def test_flexible_query_supports_terms_phrases_fields_exclusion_and_numeric_ops():
    script = textwrap.dedent(
        """
        const utils = require('./audit_react/table-utils.js');
        const rows = [
          { id: '1', desc: 'CERVEJA HEINEKEN LATA 350ML', ncm: '22030000', score: 100, bloco: 14 },
          { id: '2', desc: 'CERVEJA HEINEKEN LONG NECK 330ML', ncm: '22030000', score: 87, bloco: 15 },
          { id: '3', desc: 'PINCEL ROMA 301 005', ncm: '96034090', score: 74, bloco: 16 },
        ];
        const cases = {
          terms: rows.filter(r => utils.rowMatchesFlexibleQuery(r, 'heineken lata')).map(r => r.id),
          phrase: rows.filter(r => utils.rowMatchesFlexibleQuery(r, '"CERVEJA HEINEKEN"')).map(r => r.id),
          field: rows.filter(r => utils.rowMatchesFlexibleQuery(r, 'ncm:96034090')).map(r => r.id),
          exclusion: rows.filter(r => utils.rowMatchesFlexibleQuery(r, 'heineken -long')).map(r => r.id),
          numeric: rows.filter(r => utils.rowMatchesFlexibleQuery(r, 'score>=90 bloco=14')).map(r => r.id),
          alternative: rows.filter(r => utils.rowMatchesFlexibleQuery(r, 'pincel|long')).map(r => r.id),
        };
        console.log(JSON.stringify(cases));
        """
    )

    assert run_node(script) == {
        "terms": ["1"],
        "phrase": ["1", "2"],
        "field": ["3"],
        "exclusion": ["1"],
        "numeric": ["1"],
        "alternative": ["2", "3"],
    }


def test_column_filters_and_column_profile_preserve_full_table_visibility():
    script = textwrap.dedent(
        """
        const utils = require('./audit_react/table-utils.js');
        const rows = [
          { id: '1', desc: 'CERVEJA HEINEKEN LATA 350ML', ncm: '22030000', score: 100 },
          { id: '2', desc: 'PINCEL ROMA 301 005', ncm: '96034090', score: 74 },
        ];
        const filtered = utils.applyColumnFilters(rows, {
          desc: { value: 'heineken', operator: 'contains' },
          score: { value: '90', operator: '>=' },
        }).map(r => r.id);
        const columns = [
          { key: 'id' },
          { key: 'desc' },
          { key: 'ncm' },
          { key: 'score' },
        ];
        const profiled = utils.applyColumnProfile(columns, {
          visible: ['score', 'desc'],
          order: ['desc', 'score'],
        }).map(c => c.key);
        const complete = utils.applyColumnProfile(columns, {
          visible: null,
          order: ['ncm'],
        }).map(c => c.key);
        console.log(JSON.stringify({ filtered, profiled, complete }));
        """
    )

    assert run_node(script) == {
        "filtered": ["1"],
        "profiled": ["desc", "score"],
        "complete": ["ncm", "id", "desc", "score"],
    }
