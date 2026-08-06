#!/usr/bin/env python3
"""Regenerate `.sqlx/` without a database. **A fallback, not the normal tool.**

`cargo sqlx prepare` is how the offline cache is meant to be produced, and it is
what CONTRIBUTING tells you to run: it asks a real PostgreSQL what each query
returns, so it cannot disagree with the database. Use it whenever you have
Docker.

This script exists for the case where you do not. It extracts every
`sqlx::query!` literal from `src/lib.rs` and emits each cache entry from the
table of columns below, which is a hand-maintained transcription of the
migrations. That transcription is the risk: if it drifts from the migrations,
the crate still compiles and then fails at runtime against a real database.

Two things make that risk smaller than it looks:

* `cargo check` is a real check. sqlx generates the Rust type from the cache, so
  a wrong `type_info` or `nullable` produces `Option<T>` where the code wants
  `T` and the build fails.
* The integration suites run every query against a real PostgreSQL, so a schema
  mismatch surfaces there.

**Anything generated here must be confirmed by a real `cargo sqlx prepare`
before release.** If this script and `cargo sqlx prepare` disagree, the database
is right.

Usage, from the workspace root:

    python3 crates/batchflow-postgres/tools/generate_sqlx_cache.py
"""

import re, json, hashlib, os, glob

src = open('crates/batchflow-postgres/src/lib.rs').read()
plain = re.findall(r'sqlx::query!\(\s*"((?:[^"\\]|\\.)*)"', src, re.S)
raw = re.findall(r'sqlx::query!\(\s*r#"(.*?)"#', src, re.S)
# Raw strings have no escapes to undo; plain ones do.
queries = [q.encode().decode('unicode_escape') for q in plain] + raw
unique = sorted(set(queries))
print(f"{len(queries)} query literals, {len(unique)} distinct")

COLS = {
    'job_execution': {
        'id': ('Int8', False), 'instance_id': ('Int8', False),
        'status': ('Text', False), 'execution_context': ('Jsonb', False),
        'created_at': ('Timestamptz', False), 'ended_at': ('Timestamptz', True),
        'last_updated': ('Timestamptz', False), 'exit_message': ('Text', True),
    },
    'step_execution': {
        'id': ('Int8', False), 'job_execution_id': ('Int8', False),
        'step_name': ('Text', False), 'status': ('Text', False),
        'read_count': ('Int8', False), 'write_count': ('Int8', False),
        'filter_count': ('Int8', False), 'skip_count': ('Int8', False),
        'execution_context': ('Jsonb', False),
        'created_at': ('Timestamptz', False), 'ended_at': ('Timestamptz', True),
        'last_updated': ('Timestamptz', False), 'exit_message': ('Text', True),
    },
    'job_instance': {
        'id': ('Int8', False), 'job_name': ('Text', False),
        'parameters': ('Jsonb', False),
    },
}

def cols_for(table, names):
    out = [{"ordinal": i, "name": n, "type_info": COLS[table][n][0],
            "origin": {"Table": {"table": table, "name": n}}}
           for i, n in enumerate(names)]
    return out, [COLS[table][n][1] for n in names]

def entry(query, table=None, names=(), params=(), expression=None):
    columns, nullable = expression if expression is not None else cols_for(table, names)
    return {"db_name": "PostgreSQL", "query": query,
            "describe": {"columns": columns, "parameters": {"Left": list(params)},
                         "nullable": nullable}}

def only(predicate):
    hits = {q for q in unique if predicate(q)}
    assert len(hits) == 1, len(hits)
    return hits.pop()

def find(*fragments):
    hits = {q for q in unique if all(f in q for f in fragments)}
    assert len(hits) == 1, (fragments, len(hits))
    return hits.pop()

JE = ['id','instance_id','status','execution_context','created_at','ended_at','last_updated','exit_message']
SE = ['id','job_execution_id','step_name','status','read_count','write_count',
      'filter_count','skip_count','execution_context','created_at','ended_at',
      'last_updated','exit_message']

specs = [
    entry(find('FROM job_execution','ORDER BY id DESC'), 'job_execution', JE, ['Int8']),
    # `executions` ends with a bare `ORDER BY id`; `last_execution` with DESC.
    entry(only(lambda q: 'FROM job_execution' in q and q.rstrip().endswith('ORDER BY id')),
          'job_execution', JE, ['Int8']),
    entry(only(lambda q: q.startswith('UPDATE job_execution')), params=['Int8','Text','Jsonb','Bool','Text'], expression=([], [])),
    entry(find('INSERT INTO job_execution'), 'job_execution',
          ['id','created_at','last_updated'], ['Int8','Text','Jsonb']),
    entry(find('UPDATE step_execution'),
          params=['Int8','Text','Int8','Int8','Int8','Int8','Jsonb','Bool','Text'], expression=([], [])),
    entry(find('INSERT INTO step_execution'), 'step_execution',
          ['id','created_at','last_updated'], ['Int8','Text','Text','Jsonb']),
    entry(find('FROM step_execution s'), 'step_execution', SE, ['Int8','Text']),
    entry(only(lambda q: 'FROM step_execution' in q and 'JOIN' not in q and q.startswith('SELECT')),
          'step_execution', SE, ['Int8']),
    entry(find('FROM job_instance WHERE id'), 'job_instance', ['id'], ['Int8']),
    entry(find('WITH locked AS'), params=['Int8','Text','Text'], expression=(
        [{"ordinal":0,"name":"status?","type_info":"Text","origin":"Expression"},
         {"ordinal":1,"name":"updated?","type_info":"Int8","origin":"Expression"}],
        [None, None])),
    entry(find('INSERT INTO job_instance'), 'job_instance',
          ['id','job_name','parameters'], ['Text','Jsonb']),
    entry(find('SELECT id, job_name, parameters\n'), 'job_instance',
          ['id','job_name','parameters'], ['Text','Jsonb']),
]

for f in glob.glob('crates/batchflow-postgres/.sqlx/query-*.json'):
    os.remove(f)

written = set()
for spec in specs:
    h = hashlib.sha256(spec['query'].encode()).hexdigest()
    spec['hash'] = h
    with open(f"crates/batchflow-postgres/.sqlx/query-{h}.json", 'w') as fh:
        json.dump(spec, fh, indent=2); fh.write("\n")
    written.add(spec['query'])

missing = [q for q in unique if q not in written]
print(f"wrote {len(written)} entries; {len(missing)} uncovered")
for m in missing:
    print("  UNCOVERED:", ' '.join(m.split())[:90])
