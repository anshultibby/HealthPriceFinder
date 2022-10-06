import json

import pandas as pd


def iterate_for_df(parser, df_row, iter_step_size):

    procedure_keys = ['negotiation_arrangement', 'name', 'billing_code_type', 'billing_code_type_version', 'billing_code', 'description']
    negotiated_rate_keys = ['negotiated_rate', 'expiration_date', 'provider_references', 'negotiated_type', 'billing_class']


    main_df = pd.DataFrame()
    procedure_df = pd.DataFrame()
    procedure_dict = dict(df_row)
    for i in range(iter_step_size):
        prefix, event, value = next(parser)
        if prefix == "in_network.item.negotiated_rates.item.negotiated_prices.item.service_code.item":
            continue
        # print(prefix, event, value)

        for k in procedure_keys:

            if prefix == f"in_network.item.{k}":
                df_row[k] = [value]
                continue

        if prefix == "in_network.item.negotiated_rates":
            if event == "end_array" or event == "start_array":
                procedure_dict = dict(df_row)

        if prefix == "in_network.item.negotiated_rates.item":
            if event == "start_map":
                provider_references_list = []
            if event == "end_map":
                provider_references_list = []

        if procedure_dict["name"] != [None]:
            for k in negotiated_rate_keys:

                if k == "provider_references":
                    if prefix == f"in_network.item.negotiated_rates.item.{k}.item":
                        provider_references_list.append(value)
                        s = json.dumps(provider_references_list)
                        procedure_dict[k] = [s]

                else:
                    if prefix == f"in_network.item.negotiated_rates.item.negotiated_prices.item.{k}":
                        procedure_dict[k] = [value]

            if prefix == f"in_network.item.negotiated_rates.item.negotiated_prices.item.billing_class":
                procedure_df = pd.DataFrame(procedure_dict)
                # print(procedure_dict, procedure_df, prefix)

                main_df = pd.concat((main_df, procedure_df))

    return main_df
