#!/usr/bin/env python
# coding: utf-8


def update_query(table: str, field: str, is_jsonb: bool, from_string: str,
                 to_string: str):
    const = f'UPDATE {table} SET "{field}" = replace({field}'  # noqa
    if is_jsonb:
        query = f'{const}::TEXT, \'{from_string}\', \'{to_string}\')::jsonb;'
    else:
        query = f'{const}, \'{from_string}\', \'{to_string}\');'
    return query


def generate_queries(from_string: str, to_string: str):
    data = {
        "project": [{
            "field": "label_config",
            "is_jsonb": False
        }, {
            "field": "control_weights",
            "is_jsonb": True
        }, {
            "field": "parsed_label_config",
            "is_jsonb": True
        }],
        "prediction": [{
            "field": "result",
            "is_jsonb": True
        }],
        "projects_projectsummary": [{
            "field": "created_labels",
            "is_jsonb": True
        }],
        "task_completion": [{
            "field": "result",
            "is_jsonb": True
        }, {
            "field": "prediction",
            "is_jsonb": True
        }],
        "tasks_annotationdraft": [{
            "field": "result",
            "is_jsonb": True
        }]
    }

    for k, v in data.items():
        for x in v:
            q = update_query(table=k,
                             field=x['field'],
                             is_jsonb=x['is_jsonb'],
                             from_string=from_string,
                             to_string=to_string)
            print(q + '\n')

    print('--', '-' * 77)

    for k, v in data.items():
        for x in v:
            check_exist = f'SELECT * FROM {k} WHERE {x["field"]}::TEXT ' \
                          f'LIKE \'%{from_string}%\';'
            print(check_exist + '\n')
    return
