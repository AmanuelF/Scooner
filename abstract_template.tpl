{
    "from": {{ skip }},
    "query": {
        "bool": {
            "must": [{
                "function_score": {
                    "query": {
                        "bool": {
                            "should": [{
                                "query_string": {
                                     "query": "keywords:{{keywords}}",
                                     "default_operator": "OR"
                                }
                            },{
                                "query_string": {
                                    "query": "keywords:{{keywords}}",
                                    "default_operator": "AND"
                                }
                            }
                            ]
                        }
                    }
                }
            }]
        }

    }
}

                           
