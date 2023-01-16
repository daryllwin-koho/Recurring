# This script creates the Month over Month view for the CoverMe portfolio

# Revision: Aug 21, 2022

# This version of the script regenerates the whole file from scratch.
# Use this version of the script if some data issues have been identified (after the issues have been fixed)
# in the underlying feeder files
#
#
# This version of the script includes "do not collect" logic wherein a user
# remains current if they make the $5 fee payment every month

import pandas as pd
import pandas_gbq as pgbq
from time import strftime
import datetime
import numpy as np

from google.oauth2 import service_account
#credentials = service_account.Credentials.from_service_account_file(
#    './tensile-oarlock-191715-7c8e26b6cf77.json')

#pgbq.context.credentials = credentials

project_id = 'tensile-oarlock-191715'

# The current reporting month end
# Modify as needed when new script is run
curr_month_end = str(datetime.date.today() - datetime.timedelta(days=datetime.date.today().weekday() + 1))

# The name of the CSV output file. Eventually, this should be commented out.
# Modify as needed when new script is run
csv_out_name = 'coverme_finance_weekly_view_2022_10_31_mst_2.csv'

# This is the output table that we write to in MetaBase
# Do not modify this
out_table = 'risk.CM_finance_weekly_view_alt'

# Cohort ends
# Do not modify the start date
# Add a month to the end date
# Requirement for end date to be several periods in the future 
# as due date of paid loans could be up to 30 days away

next_month = datetime.date.today().replace(day=28) + datetime.timedelta(days=4)
last_day = next_month - datetime.timedelta(days=next_month.day)

periods = pd.date_range(start='2021-07-31', 
                       end=str(last_day),
                       freq='W') # W for weekly M for monthly
periods

def loans_by_period(start, end):
    
    # Quite a few variables have "month" in their name. Going through and changing to "week"
    # would be a pain in the ass so leaving as "month" for now.
    # For example, repayments_in_month only captures the repayments made in the last week
    
    query = """with
    
                mapping as (select 
                                distinct account_group_identifier, 
                                    user_reference 
                                    from accounts.kledger_accounts group by 1,2),
                                    
                transactions as (select 
                    b.user_reference,
                    a.UserAccount,
                    a.TransactionID,
                    date(PostedAt, 'America/Denver') as PostedAt,
                    
                    -- This changes to ISOWEEK
                    
                    last_day(date(a.PostedAt, 'America/Denver'), ISOWEEK) as t_month,
                    case
                        when a.PaymentCode in ('ODR-DR-A-E-001') then '1. fee'
                        when a.PaymentCode in ('ODR-CR-A-E-001') then '2. loan'
                        else '3. repayment'
                    end as TType,
                    case 
                        when a.PaymentCode in ('ODR-DR-A-E-002', 'ODR-DR-A-E-001') then -1*cast(trim(a.Amount,"$") as FLOAT64)
                        else cast(trim(a.Amount,"$") as FLOAT64)
                    end as Amount
                    from kohoapi.transaction_succeeded_events as a
                        left join mapping as b on a.UserAccount = b.account_group_identifier
                    where PaymentCode in ('ODR-DR-A-E-001','ODR-CR-A-E-001','ODR-DR-A-E-002') and date(PostedAt, 'America/Denver') <= '{1}'),
                
                last_loan as 
                    (select 
                        user_reference, 
                        max(PostedAt) as last_loan_date 
                    from transactions where TType = '2. loan' group by 1),
                
                -- need this to make sure that we don't somehow have to deal with an error where 
                -- a re-payment is the very first transaction
                first_loan as 
                    (select 
                        user_reference, 
                        min(PostedAt) as first_loan_date 
                    from transactions where TType = '2. loan' group by 1),
                
                loans_in_month as 
                    (select 
                        distinct user_reference, 
                        1 as loan, 
                        sum(Amount) as loaned_in_month 
                    from transactions where TType = '2. loan' AND t_month = '{1}' group by 1),
                
                repayments_in_month as 
                    (select 
                        distinct user_reference, 
                        count(*) as num_repayment, 
                        sum(Amount) as repaid_in_month 
                            from transactions where TType = '3. repayment' AND t_month = '{1}' group by 1),
                
                OS as 
                    (select 
                        distinct a.user_reference, 
                        sum(a.Amount) as OS 
                    from transactions as a 
                    inner join first_loan as b 
                        on a.user_reference = b.user_reference 
                            and a.PostedAt >= b.first_loan_date where TType in ('2. loan', '3. repayment') group by 1),
                
                last_fee as 
                    (select 
                        user_reference, 
                        max(PostedAt) as last_fee_date 
                    from transactions where TType = '1. fee' group by 1),
                    
                last_payment as
                    (select 
                        user_reference, 
                        max(PostedAt) as last_payment_date 
                    from transactions where TType = '3. repayment' group by 1),
                
                -- counting fee payments after last disbursal
                
                count_fee as 
                    (select 
                        distinct a.user_reference, 
                        count(a.TType) as num_fee_payments 
                    from transactions as a 
                    inner join last_loan as b 
                        on a.user_reference = b.user_reference 
                            and date(a.PostedAt) > date(last_loan_date) where TType = '1. fee' group by 1),
                        
                -- counting fee payments that were SUPPOSED to be made after disbursal
                        
                count_fee_s as 
                    (select 
                        user_reference, 
                        last_loan_date, 
                        date_diff(date('{1}'),date(last_loan_date), DAY)/30 as diff from last_loan),
                
                final as (select date('{1}') as snapshot, 
                            a.user_reference,
                            b.OS,
                            c.last_loan_date,
                            d.last_fee_date,
                            i.last_payment_date,
                            IFNULL(g.num_fee_payments,0) as num_fee_payments,
                            floor(h.diff) as num_expected_fee_payments,
                            date_add(date(c.last_loan_date), INTERVAL 30 DAY) as orig_due_date,
                            case
                                when date(c.last_loan_date) <= '2022-05-01' then date_add(date(c.last_loan_date), INTERVAL 30 DAY)
                                
                                -- Need to add IFNULL on the date as somehow the last fee is not captured
                                
                                when date(c.last_loan_date) > '2022-05-01' AND IFNULL(g.num_fee_payments,0) >= floor(h.diff) then date_add(date(IFNULL(d.last_fee_date, c.last_loan_date)), INTERVAL 30 DAY)
                                when date(c.last_loan_date) > '2022-05-01' AND IFNULL(g.num_fee_payments,0) < floor(h.diff) then date_add(date(c.last_loan_date), INTERVAL ((1+IFNULL(g.num_fee_payments,0))*30) DAY)
                            end as due_date,
                            
                            
                            case
                                when i.last_payment_date IS NOT NULL AND date(i.last_payment_date) >= date(c.last_loan_date) then date(i.last_payment_date)
                                else date(c.last_loan_date)   
                            end as update_date,
                            
                            e.loan as num_loans_in_month,
                            e.loaned_in_month as total_loaned_in_month,
                            f.num_repayment as num_repayments_in_month,
                            -f.repaid_in_month as total_repaid_in_month
                            from (select distinct user_reference from transactions group by 1) as a
                            left join OS as b on a.user_reference = b.user_reference
                            left join last_loan as c on a.user_reference = c.user_reference
                            left join last_fee as d on a.user_reference = d.user_reference
                            left join loans_in_month as e on a.user_reference = e.user_reference
                            left join repayments_in_month as f on a.user_reference = f.user_reference
                            left join count_fee as g on a.user_reference = g.user_reference
                            left join count_fee_s as h on a.user_reference = h.user_reference
                            left join last_payment as i on a.user_reference = i.user_reference),
 -- Here is where we calculate the delinquency buckets                           
 final2 as (select 
    *,
    last_day(date_sub(date('{1}'), INTERVAL 1 WEEK), ISOWEEK) as prev_snap,
    last_day(date(last_loan_date), ISOWEEK) as cohort_end,
    last_day(date(last_payment_date), ISOWEEK) as cohort_paid,
    case
        when OS = 0 then "1. Inactive"
        when OS > 0 AND date(due_date) >= date('{1}') then "2. current"
        when OS > 0 and date_diff(date('{1}'),date(due_date),DAY) between 1 and 30 then '3. 1 to 30'
        when OS > 0 and date_diff(date('{1}'),date(due_date),DAY) between 31 and 60 then '4. 31 to 60'
        when OS > 0 and date_diff(date('{1}'),date(due_date),DAY) between 61 and 90 then '5. 61 to 90'
        when OS > 0 and date_diff(date('{1}'),date(due_date),DAY) > 90 then '6. 91+'
        else "7. Balance Issue"
    end as snapshot_status from final where OS IS NOT NULL),
cm_trans as (select * from transaction.kledger_transaction_lines where date(TIMESTAMP) >= '2022-10-12'),   
grad as (select user_reference,TIMESTAMP,description from cm_trans where description = "Cover Funds (Limit Upgrade)"),

CMG as (select *, case when user_reference in (select user_reference from grad) then 1 else 0 end as grad_flag from final2)

select *, case when user_reference in (select string_field_0 from credit.ep_migration_list) then 1 else 0 end as migration_flag from CMG
  
""".format(start, end)

    loans = pgbq.read_gbq(query, project_id, dialect='standard')
    
    return loans

# initializing curr_loans
curr_loans = loans_by_period(periods[0].strftime('%Y-%m-%d'),periods[periods.size-1].strftime('%Y-%m-%d'))

curr_loans.head()

# creating stacked dataset
i = 1
while i < periods.size-1:
    curr_loans = pd.concat([curr_loans, loans_by_period(periods[0].strftime('%Y-%m-%d'),periods[i].strftime('%Y-%m-%d'))], ignore_index=True)
    i += 1
    print( str(i) + " out of " + str(periods.size-1))

# output the results to a spreadsheet
curr_month_end = datetime.datetime.strptime(curr_month_end, '%Y-%m-%d')

for i in range(len(curr_loans.columns)):
    if(curr_loans.dtypes.values[i] == "dbdate"):
        curr_loans[curr_loans.columns[i]]= pd.to_datetime(curr_loans[curr_loans.columns[i]])

# Delete snapshots that are greater than the current month
curr_loans = curr_loans[curr_loans.snapshot <= curr_month_end]
curr_loans = curr_loans.drop_duplicates()
# curr_loans.to_csv('new_pay_logic_test_2.csv', index=False)

# curr_loans.to_csv('new_weekly_pay_logic_test.csv', index=False)

curr_loans['snapshot_status'].value_counts()

curr_loans['days_between_paid_and_due'] = ((curr_loans['update_date'] - curr_loans['due_date'])/np.timedelta64(1,'D')).astype(int)

# Self Merge to get previous status

curr_loans_copy = curr_loans[['snapshot', 'user_reference', 'snapshot_status']]
curr_loans = curr_loans.merge(curr_loans_copy, how='left', left_on=["user_reference", "prev_snap"], right_on=["user_reference","snapshot"], suffixes=('_left', '_right'))
curr_loans.drop(['snapshot_right'],axis=1,inplace=True)
curr_loans = curr_loans.rename({'snapshot_left': 'snapshot', 'snapshot_status_left': 'snapshot_status','snapshot_status_right' :  'prev_status'}, axis='columns')
curr_loans['prev_status'].fillna('0. non existent', inplace = True)
curr_loans.head()

curr_loans_2 = curr_loans.copy(deep=True)

def get_new(row):
    if row['snapshot'] == row['cohort_end']:
        return 'Yes'
    else:
        return 'No'

def get_paid_class(row):
    if row['snapshot_status'] == '1. Inactive':
        if row['days_between_paid_and_due'] <= 0:
            return '1. paid on time'
        elif 0 < row['days_between_paid_and_due'] < 91:
            return '2. paid late'
        elif row['prev_status'] == '6. 91+':
            return '3. paid after default'
        else:
            return '2. paid late'
    else: 
        return '4. NA'

def get_default(row):
    if row['snapshot_status'] == '6. 91+':
        if row['prev_status'] != '6. 91+':
            return '1. Yes'
        else:
            return '2. No'
    else:
        return '3. NA'

def get_paid_cohort(row):
    if row['snapshot_status'] == '1. Inactive':
        if row['cohort_paid'] == row['snapshot']:
            return '1. Yes'
        else:
            return '2. No'
    else:
            return '3. NA'

curr_loans_2['new_loan'] = curr_loans_2.apply(get_new, axis=1)
curr_loans_2['paid_class'] = curr_loans_2.apply(get_paid_class, axis=1)
curr_loans_2['new_default'] = curr_loans_2.apply(get_default, axis=1)
curr_loans_2['paid_in_cohort'] = curr_loans_2.apply(get_paid_cohort, axis=1)
curr_loans_2.head()

for i in range(len(curr_loans_2.columns)):
    if(curr_loans_2.dtypes.values[i] == "dbdate"):
        curr_loans_2[curr_loans_2.columns[i]]= pd.to_datetime(curr_loans_2[curr_loans_2.columns[i]])

# Delete snapshots that are greater than the current month
final = curr_loans_2[curr_loans_2.snapshot <= curr_month_end]


# Sort
final.sort_values(['user_reference', 'snapshot'], inplace = True)        
            
# drop columns
# final.drop(labels=['loan_type','status','prev_snap','days_between_curr_and_dep','days_between_paid_and_due','days_between_curr_and_paid','days_between_curr_and_due'],axis=1, inplace=True)
final.drop(labels=['days_between_paid_and_due'],axis=1, inplace=True)

# delete extra paid status rows
# final.drop(final[(final.snapshot_status == '6. paid') and (final.snapshot_status == final.snapshot_status)].index, inplace=True)
final_dedup = final.drop_duplicates()

# reset index
final_dedup.reset_index(drop=True, inplace=True)

# renaming columns
final_dedup.rename(columns={"num_loans_in_month": "num_loans_in_week", "total_loaned_in_month": "total_loaned_in_week", "num_repayments_in_month":"num_repayments_in_week","total_repaid_in_month":"total_repaid_in_week"},inplace=True)

# output the results to a spreadsheet
final_dedup.to_csv(csv_out_name, index=False)


final_dedup.head()

curr_loans_2.dtypes

# Upload data back to the cloud 
pgbq.to_gbq(final_dedup, 'risk.CM_finance_weekly_view_new', project_id, if_exists='replace')