import servicenow_api_tools.mock_api_server.parsers as parsers
from lark import Tree, Token


def test_parse_sysparm_query():
    query = (
        'foo=bar^ORactive=true,person.date_of_birthISNOTEMPTY^'
        'person.date_of_birthBETWEEN'
        'javascript:gs.daysAgoStart(25)@javascript:var myTruth = '
        'Object.getOwnPropertyNames(this).length < 1; if (myTruth) { gs.beginningOfLastWeek() }')
    expected = Tree('query', [
        Tree('and', [
            Tree('query', [
                Tree('or', [
                    Tree('matcher', [
                        Tree('is', [
                            Token('FIELD', 'foo'),
                            Token('VALUE', 'bar')
                        ])
                    ]),
                    Tree('matcher', [
                        Tree('is', [
                            Token('FIELD', 'active'),
                            Token('VALUE', 'true,person.date_of_birthISNOTEMPTY')
                        ])
                    ])
                ])
            ]),
            Tree('query', [
                Tree('matcher', [
                    Tree('date_between', [
                        Token('FIELD', 'person.date_of_birth'),
                        Token('VALUE', 'javascript:gs.daysAgoStart(25)'),
                        Token('VALUE', (
                            'javascript:var myTruth = Object.getOwnPropertyNames(this).length '
                            '< 1; if (myTruth) { gs.beginningOfLastWeek() }'))
                    ])
                ])
            ])
        ])
    ])

    result = parsers.parse_sysparm_query(query)
    assert expected.pretty() == result.pretty()


def test_parse_sysparm_fields():
    fields = "foo,bar"
    expected = Tree('fields', [
        Token('FIELD', 'foo'),
        Token('FIELD', 'bar')
    ])
    result = parsers.parse_sysparm_fields(fields)
    assert expected.pretty() == result.pretty()


def test_parse_sysparm_group_by():
    fields = "foo,bar"
    expected = Tree('fields', [
        Token('FIELD', 'foo'),
        Token('FIELD', 'bar')
    ])
    result = parsers.parse_sysparm_group_by(fields)
    assert expected.pretty() == result.pretty()


def test_parse_sysparm_offset():
    offset = "213545"
    expected = Tree('offset', [
        Token('OFFSET', "213545")
    ])
    result = parsers.parse_sysparm_offset(offset)
    assert expected.pretty() == result.pretty()


def test_parse_sysparm_limit():
    limit = "213545"
    expected = Tree('limit', [
        Token('LIMIT', "213545")
    ])
    result = parsers.parse_sysparm_limit(limit)
    assert expected.pretty() == result.pretty()


def test_parse_stats_query_url():
    url = (
        "https://test.servicenowservices.com/api/now/stats/activity?"
        "sysparm_group_by=activity_type&"
        "sysparm_query=active=true^checked_out=No&"
        "sysparm_count=true&sysparm_display_value=false")
    expected = {
        "endpoint": "stats",
        "table": "activity",
        "group_by": Tree('fields', [
            Token('FIELD', 'activity_type')
        ]),
        "query": Tree('query', [
            Tree('and', [
                Tree('query', [
                    Tree('matcher', [
                        Tree('is', [
                            Token('FIELD', 'active'),
                            Token('VALUE', 'true')
                        ])
                    ])
                ]),
                Tree('query', [
                    Tree('matcher', [
                        Tree('is', [
                            Token('FIELD', 'checked_out'),
                            Token('VALUE', 'No')
                        ])
                    ])
                ])
            ])
        ])}
    result = parsers.parse_stats_query_url(url)
    assert result['endpoint'] == expected['endpoint']
    assert result['table'] == expected['table']
    assert result['group_by'].pretty() == expected['group_by'].pretty()
    assert result['query'].pretty() == expected['query'].pretty()
    result2 = parsers.parse_url(url)
    assert result2['endpoint'] == expected['endpoint']
    assert result2['table'] == expected['table']
    assert result2['group_by'].pretty() == expected['group_by'].pretty()
    assert result2['query'].pretty() == expected['query'].pretty()


def test_parse_table_query_url():
    url = (
        "https://test.servicenowservices.com/api/now/table/person?"
        "sysparm_display_value=all&sysparm_exclude_reference_link=False&"
        "sysparm_query=active=true&sysparm_fields=sys_id,date_of_birth&"
        "sysparm_offset=42000&sysparm_limit=2000")
    expected = {
        "endpoint": "table",
        "table": "person",
        "limit": Tree('limit', [
            Token('LIMIT', "2000")
        ]),
        "offset": Tree('offset', [
            Token('OFFSET', "42000")
        ]),
        "fields": Tree('fields', [
            Token('FIELD', 'sys_id'),
            Token('FIELD', 'date_of_birth')
        ]),
        "query": Tree('query', [
            Tree('matcher', [
                Tree('is', [
                    Token('FIELD', 'active'),
                    Token('VALUE', 'true')
                ])
            ])
        ])}
    result = parsers.parse_table_query_url(url)
    assert result['endpoint'] == expected['endpoint']
    assert result['table'] == expected['table']
    assert result['limit'].pretty() == expected['limit'].pretty()
    assert result['offset'].pretty() == expected['offset'].pretty()
    assert result['fields'].pretty() == expected['fields'].pretty()
    assert result['query'].pretty() == expected['query'].pretty()
    result2 = parsers.parse_url(url)
    assert result2['endpoint'] == expected['endpoint']
    assert result2['table'] == expected['table']
    assert result2['limit'].pretty() == expected['limit'].pretty()
    assert result2['offset'].pretty() == expected['offset'].pretty()
    assert result2['fields'].pretty() == expected['fields'].pretty()
    assert result2['query'].pretty() == expected['query'].pretty()
