from utils import *
import argparse
from progress.bar import Bar
from openpyxl import load_workbook
import io


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("from_date", help="Start Date")
    ap.add_argument("to_date", help="End Date")
    ap.add_argument("create", help="query or create")
    ap.add_argument("type", help="volume or time")
    ap.add_argument("trading_sessions", help="total sessions")
    args = ap.parse_args()
    print(args)
    token = login()
    from_date = args.from_date  # "2021-03-01"
    to_date = args.to_date  #"2021-03-31"
    trading_sessions = int(args.trading_sessions)
    type = args.type
    create = args.create
    if type == 'volume':
        report_name = 'volume_report_volume_task'
        excel_file = ExcelWriter('volume.xlsx', engine='xlsxwriter')
    elif type == 'time':
        report_name = 'time_report_time_task'
        excel_file = ExcelWriter('time.xlsx', engine='xlsxwriter')
    if create == 'create':
        reports_id = create_reports(token, from_date, to_date, type)
        print(reports_id)
        #reports_created = get_reports(token, reports_id)
    elif create == 'query':
        # query all reports, searh by date
        market_members = pd.DataFrame(get_market_member(token))
        market_maker = pd.DataFrame(get_market_maker(token)[0])
        print(market_members['name'][market_members['id'].isin(market_maker['market_member'])])  # print MMKs name
        reports_data, reports = get_all_reports(token, type)
        reports_data = pd.DataFrame(reports_data, columns=['from_date', 'to_date', 'report_type', 'market_member', 'state', 'task_id', 'created'])
        task_id = reports_data['task_id'][
            (reports_data.from_date.isin([from_date])) & (reports_data.to_date.isin([to_date])) & (
                    reports_data.state == 'SUCCESS')]
        print(*task_id, sep='\n')
        #searched_reports = get_reports(token, task_id.dropna())
        report_fields = None
        if not task_id.empty:
            #df_all_reports = pd.DataFrame([])
            bar = Bar('Step', max=task_id.dropna().__len__())
            for id in task_id.dropna().tolist():
                name_mmk = None
                for r in reports.json():
                    if r['task_id'] == id and r[report_name]: #hay algo en r[report_name]
                        if report_fields is None:
                            report_fields = list(r[report_name][0].keys())
                            report_fields.extend(['market_member', 'from_date', 'to_date'])
                            df_all_reports = pd.DataFrame([], columns=report_fields)

                        temp = pd.DataFrame(r[report_name])
                        name_mmk = market_members[market_members['id'] == r['market_member']]['name'].values[0]
                        temp['market_member'] = r['market_member'].__str__()
                        temp['from_date'] = r['from_date'].__str__()
                        temp['to_date'] = r['to_date'].__str__()
                        df_all_reports = df_all_reports.append(temp)
                        pd.DataFrame(r[report_name]).apply(lambda x: convert(x)).to_excel(excel_file,
                                                                                          sheet_name=name_mmk,
                                                                                          index=False)


                        #excel_mmk.close()
                        #format1 = excel_file.book.add_format({'num_format': '0'})
                        #sh = excel_file.sheets[name_mmk]
                        #sh.set_column('C:O', None, format1)
                        break
                if name_mmk:
                    try:
                        book = load_workbook(name_mmk + ".xlsx")
                        excel_mmk = ExcelWriter(name_mmk + ".xlsx", engine='openpyxl')
                        excel_mmk.book = book
                    except:
                        excel_mmk = ExcelWriter(name_mmk + ".xlsx", engine='openpyxl')
                        # pd.DataFrame(r[report_name]).apply(lambda x: convert(x)).to_excel(excel_mmk,
                        #                                                               sheet_name="Reporte volumen" if type=="volume" else "Tiempo en pantalla",
                        #                                                               index=False)

                    r = get_xls(token, type, id)
                    report_df = pd.read_excel(io.BytesIO(r.content), header=[0, 1], engine="openpyxl")
                    if type == 'time':
                        report_df.to_excel(excel_mmk,
                                           sheet_name="Reporte volumen" if type == "volume" else "Tiempo en pantalla")
                    elif type == 'volume':
                        total_member = get_volume_total(token, temp, trading_sessions)
                        total_member['ADV'] = total_member['ADV'].round(0)
                        total_member['RatioAgresor'] = total_member['RatioAgresor'].round(2)
                        total_member['Fecha_Desde'] = from_date
                        total_member['Fecha_Hasta'] = to_date
                        total_member[['ADV', 'RatioAgresor', 'Fecha_Desde', 'Fecha_Hasta']].to_excel(excel_mmk,
                                           sheet_name='Resumen')
                        fields = list(map(lambda x: x.replace('Agredio','%'), report_df.columns.levels[1].to_list()))
                        d = dict(zip(report_df.columns.levels[1], fields))
                        report_df = report_df.rename(columns=d, level=1)
                        report_df.to_excel(excel_mmk,
                                           sheet_name="Reporte volumen" if type == "volume" else "Tiempo en pantalla")
                    excel_mmk.save()

                bar.next()
            else:
                print('\n')
                print('Saving ...\n')
                df_all_reports.apply(lambda x: convert(x)).to_excel(excel_file, sheet_name='all_reports', index=False)
                #excel_file.save()
                df_all_reports = df_all_reports.reset_index()
            bar.finish()


    if type == 'volume' and create == 'query':
        total_prod_member = get_volume_total(token, df_all_reports, trading_sessions)
        total_prod_member.reset_index().to_excel(excel_file, sheet_name='totals', index=False)
        print(total_prod_member)
    else:
        pass
    excel_file.save()
    #excel_file.close()

    # performance
    #products = [[p['name'], p['volume_metrics'][0]['id']] for p in get_product(token)]

