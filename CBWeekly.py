# This script creates the Month over Month view for the Credit Building portfolio

# This version of the script regenerates the whole file from scratch.
# Use this version of the script if some data issues have been identified (after the issues have been fixed)
# in the underlying feeder files

import pandas as pd
import pandas_gbq as pgbq
from time import strftime
import datetime
import numpy as np
from datetime import date

def next_weekday(d, weekday):
    days_ahead = weekday - d.weekday()
    if days_ahead <= 0: # Target day already happened this week
        days_ahead += 7
    return d + datetime.timedelta(days_ahead)



project_id = 'tensile-oarlock-191715'

# The current reporting month end
# Modify as needed when new script is run
curr_week_end = next_weekday(date.today(), -1) # 0 = Monday, 1=Tuesday, 2=Wednesday...

# The name of the CSV output file. Eventually, this should be commented out.
# Modify as needed when new script is run
csv_out_name = 'creditbuilding_finance_view_2022_03_31_mst.csv'

# This is the output table that we write to in MetaBase
# Do not modify this
out_table = 'credit.CB_finance_view_weekly'

# Cohort ends
# Do not modify the start date
# Add a month to the end date
# Requirement for end date to be several periods in the future 
# as due date of paid loans could be up to 30 days away
periods = pd.date_range(start='2022-04-01', 
                       end=curr_week_end,
                       freq='W') # W for weekly M for monthly
periods

def loans_by_period(start, end):
    
    query = """with all_trans as (select 
    distinct b.user_reference, 
    a.UserAccount 
    from kohoapi.transaction_succeeded_events as a
                left join 
                (select distinct account_group_identifier, user_reference from accounts.kledger_accounts group by 1,2) as b
            on a.UserAccount = b.account_group_identifier
    where a.PaymentCode in ('CRB-CR-A-E-001','CRB-DR-A-E-001','CRB-DR-A-E-002','CRB-TR-A-I-001','CRB-TR-A-I-002') 
        and date(a.PostedAt,'America/Denver') <= '{1}'
    group by 1,2),
    
    min_date as (select
    a.account_identifier,
    min(a.created_at) as open_date
    from feature.loans as a
    inner join all_trans as b on a.account_identifier = b.UserAccount
    where a.loan_type in ('creditbuilding')
    group by 1),
    
    max_date as (select
    a.account_identifier,
    max(a.created_at) as open_date
    from feature.loans as a
    inner join all_trans as b on a.account_identifier = b.UserAccount
    where a.loan_type in ('creditbuilding')
    group by 1),
    
    all_acct as (select
    datetime(date('{1}')) as snapshot,
    b.user_reference,
    a.account_identifier,
    datetime(a.open_date,'America/Denver') as open_date,
    d.loan_type,
    d.amount as CBLimit,
    d.status,
    datetime(d.updated_at,'America/Denver') as updated_at
    from min_date as a
        inner join all_trans as b
            on a.account_identifier = b.UserAccount
        left join max_date as c
            on a.account_identifier = c.account_identifier
        inner join feature.loans as d
            on c.account_identifier = d.account_identifier and c.open_date = d.created_at
    ),

    withdraw as (select 
    b.user_reference,
    a.UserAccount, 
    DATETIME(a.PostedAt,'America/Denver') as PostedAt, 
    a.TransactionID, 
    cast(trim(a.Amount,"$") as FLOAT64) as Amount, 
    a.PaymentCode,
    'Borrow' AS TR_Desc
    from kohoapi.transaction_succeeded_events as a
        left join 
        (select distinct account_group_identifier, user_reference from accounts.kledger_accounts group by 1,2) as b
            on a.UserAccount = b.account_group_identifier
    where a.PaymentCode in ('CRB-TR-A-I-001') and date(a.PostedAt,'America/Denver') <= '{1}' 
        order by a.UserAccount, a.PostedAt),

    -- needed so that we can weed out some stray repayment transactions 
    first_loan as (select UserAccount, min(PostedAt) as first_loan_date from withdraw group by 1),
    
    repay as (select 
    b.user_reference,
    a.UserAccount, 
    DATETIME(a.PostedAt,'America/Denver') as PostedAt, 
    a.TransactionID, 
    -cast(trim(a.Amount,"$") as FLOAT64) as Amount, 
    a.PaymentCode,
    'Repay' AS TR_Desc
    from kohoapi.transaction_succeeded_events as a
        left join 
        (select distinct account_group_identifier, user_reference from accounts.kledger_accounts group by 1,2) as b
            on a.UserAccount = b.account_group_identifier
        inner join first_loan as c 
            on a.UserAccount = c.UserAccount and DATETIME(a.PostedAt,'America/Denver') >= c.first_loan_date
    where a.PaymentCode in ('CRB-TR-A-I-002') and date(a.PostedAt,'America/Denver') <= '{1}' 
        order by a.UserAccount, a.PostedAt), 
    
    raw as (select *, 

    CAST (sum(amount) 
        over (partition by user_reference 
            order by PostedAt asc rows between unbounded preceding and current row) AS FLOAT64) as OS

    from (select * from withdraw 

    union all 

    select * from repay

    order by 1,2,3)),
    
    paired_withdraw as (select 
        a.*, 
        min(b.PostedAt) as next_withdrawal_date,
        from withdraw as a 
            left join withdraw as b 
            on a.user_reference = b.user_reference 
                and a.PostedAt < b.PostedAt group by 1,2,3,4,5,6,7 order by 1,2,3),
    
    grouped_withdraw_repay as (select 
       a.*, 
        sum(b.Amount) as RepaymentAmount, 
        max(b.PostedAt) as RepaymentDate 
        from paired_withdraw as a left join repay as b 
        on a.user_reference = b.user_reference 
            and ((a.next_withdrawal_date is not null and b.PostedAt >= a.PostedAt and b.PostedAt <= a.next_withdrawal_date) 
            or (a.next_withdrawal_date is null and b.PostedAt >= a.PostedAt))
        group by 1,2,3,4,5,6,7,8),
        
    grouped_repay as (select a.user_reference, a.PostedAt as PrevWithdrawDate, a.RepaymentDate, a.RepaymentAmount  
        from grouped_withdraw_repay as a 
        inner join 
            (select user_reference, max(PostedAt) as PostedAt from grouped_withdraw_repay group by 1) as b 
        on a.user_reference = b.user_reference and a.PostedAt = b.PostedAt
        where RepaymentDate IS NOT NULL),
    
    staging as (select 
        a.user_reference, 
        b.last_TR,
        a.TR_Desc as Last_TR_Desc,
        CASE 
            when a.OS BETWEEN -0.01 and 0.01 then 0
            else a.OS
        END as OS,
        c.withdrawals as in_month_withdrawals,
        d.repayments as in_month_repayments,
        e.PrevWithdrawDate,
        -e.RepaymentAmount as RepaymentAmount,
        case
            when PrevWithdrawDate is not null then -e.RepaymentAmount + a.OS
            else a.OS
        end as prev_OS
        from raw as a
            inner join 
                (select distinct user_reference, max(PostedAt) as last_TR from raw group by 1) as b
                on a.user_reference = b.user_reference and a.PostedAt = b.last_TR
            left join 
                (select 
                    distinct user_reference, 
                    sum(Amount) as withdrawals 
                    from withdraw 
                    where last_day(date(PostedAt), week(monday)) = '{1}'
                    group by 1) as c 
            on a.user_reference = c.user_reference
            left join 
                (select 
                    distinct user_reference, 
                    -sum(Amount) as repayments 
                    from repay
                    where last_day(date(PostedAt), week(monday)) = '{1}'
                    group by 1) as d 
            on a.user_reference = d.user_reference
            left join 
                grouped_repay as e on a.user_reference = e.user_reference and a.PostedAt = e.RepaymentDate),

date_calc as (select 
    user_reference, 
    last_TR, 
    Last_TR_Desc, 
    OS, 
    in_month_withdrawals, 
    in_month_repayments, 
    PrevWithdrawDate, 
    RepaymentAmount, 
    Prev_OS,
    case
        -- Trivial case where the user owed <= $67.50 and paid it all back. 
        when PrevWithdrawDate is not null and Prev_OS <= 67.50 
            and RepaymentAmount >= Prev_OS then DATETIME_ADD(PrevWithdrawDate,INTERVAL 30 DAY)
        -- Case where the user owed <= 67.50 but did not pay back the full amount. In that case their due date is the last disbursement + 30 days
        when PrevWithdrawDate is not null and Prev_OS <= 67.50 
            and RepaymentAmount < Prev_OS then DATETIME_ADD(PrevWithdrawDate,INTERVAL 30 DAY)
        -- Trivial case where user owed more than 67.50 and paid it all back. It could be in a series of small payments.
        when PrevWithdrawDate is not null and Prev_OS > 67.50 
            and RepaymentAmount >= Prev_OS then DATETIME_ADD(PrevWithdrawDate,INTERVAL 30 DAY)
        -- The case where the user owed more than 67.50 and made the minimum payment
        when PrevWithdrawDate is not null and Prev_OS > 67.50 
            and RepaymentAmount < Prev_OS and RepaymentAmount >= GREATEST(0.3*prev_OS, 67.50) then DATETIME_ADD(last_TR,INTERVAL 30 DAY)
        -- The case where the user owed more than 67.50 and made less than the minimum payment
        when PrevWithdrawDate is not null and Prev_OS > 67.50 
            and RepaymentAmount < Prev_OS and RepaymentAmount < GREATEST(0.3*prev_OS, 67.50) then DATETIME_ADD(PrevWithdrawDate,INTERVAL 30 DAY)
        -- Last transaction was a withdrawal. It is ASSUMED that the user was allowed to withdraw (system checked user was current).
        else DATETIME_ADD(last_TR,INTERVAL 30 DAY)
    end as due_date
    from staging),
    
    accessed as (select 
        user_reference,
        last_TR,
        Last_TR_Desc,
        in_month_withdrawals, 
        in_month_repayments, 
        OS,
        due_date,
        case
            when OS > 0 and date(due_date) >= '{1}' then "1. Current"
            when OS > 0 and date(due_date) < '{1}' 
                and date_diff('{1}', date(due_date), DAY) between 1 and 30 then "2. 1 to 30"
            when OS > 0 and date(due_date) < '{1}' 
                and date_diff('{1}', date(due_date), DAY) between 31 and 60 then "3. 31 to 60"
            when OS > 0 and date(due_date) < '{1}' 
                and date_diff('{1}', date(due_date), DAY) between 61 and 90 then "4. 61 to 90"
            when OS > 0 and date(due_date) < '{1}' 
                and date_diff('{1}', date(due_date), DAY) >= 91 then "5. 91+"
            else "1. Current"
        end as status
        from date_calc)
        
select 
    a.snapshot,
    case
        when b.OS is not null then 'Yes'
        else 'No'
    end as accessed,
    a.user_reference, 
    a.account_identifier,
    date(a.open_date) as open_date,
    a.loan_type,
    a.CBLimit,
    case 
        when b.status is not null then b.status
        when b.status is null and a.status = 'cancelled' and date(a.updated_at) <= '{1}' then '6. Cancelled'
        else '1. Current'
    end as snapshot_status,
    case 
        when b.in_month_withdrawals is null then 0
        else b.in_month_withdrawals
    end as in_month_withdrawals,
    case 
        when b.in_month_repayments is null then 0
        else b.in_month_repayments
    end as in_month_repayments,
    case 
        when b.OS is null then 0
        else b.OS
    end as OS,
    case 
        when b.due_date is null then '{1}'
        else date(b.due_date)
    end as due_date,
    
    --This section of the code is modified (from the monthly run) so that it handles week over week calcs
                    
         

    last_day(date_sub(date('{1}'), INTERVAL 1 WEEK), ISOWEEK) as prev_snap,
    
    last_day(date(a.open_date), MONTH) as cohort_end_month,
    last_day(date(a.open_date), ISOWEEK) as cohort_end
    
    from all_acct as a
    left join accessed as b
        on a.user_reference = b.user_reference
    
    -- remove any stale accounts
    where not (a.status = 'cancelled' and last_day(date(a.updated_at), MONTH) < date('{1}'))""".format(start, end)

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

curr_loans.shape

# A bit of code to speed up execution later
# We split out those accounts that have been accessed (much smaller group) and treat them separately

ever_query = """select 
    distinct b.user_reference,
    a.UserAccount as account_identifier,
    'Yes' as ever_accessed
    from kohoapi.transaction_succeeded_events as a
        left join 
        (select distinct account_group_identifier, user_reference from accounts.kledger_accounts group by 1,2) as b
            on a.UserAccount = b.account_group_identifier
    where a.PaymentCode in ('CRB-TR-A-I-001')
        group by 1,2"""

ever = pgbq.read_gbq(ever_query, project_id, dialect='standard')

# adding ever accessed flag

curr_loans = curr_loans.merge(ever, how='left', left_on=["user_reference", "account_identifier"], right_on=["user_reference","account_identifier"])


curr_loans.shape

curr_loans['ever_accessed'].value_counts()

curr_loans_2 = curr_loans.copy(deep=True)

# curr_loans_2['days_between_curr_and_dep'] = ((curr_loans_2['snapshot'] - curr_loans_2['dep_date'])/np.timedelta64(1,'D')).astype(int)
# curr_loans_2['days_between_paid_and_due'] = ((curr_loans_2['update_date'] - curr_loans_2['due_date'])/np.timedelta64(1,'D')).astype(int)
# curr_loans_2['days_between_curr_and_paid'] = ((curr_loans_2['snapshot'] - curr_loans_2['update_date'])/np.timedelta64(1,'D')).astype(int)
# curr_loans_2['days_between_curr_and_due'] = ((curr_loans_2['snapshot'] - curr_loans_2['due_date'])/np.timedelta64(1,'D')).astype(int)

curr_loans_2.head()

for i in range(len(curr_loans.columns)):
    if(curr_loans.dtypes.values[i] == "dbdate"):
        curr_loans[curr_loans.columns[i]]= pd.to_datetime(curr_loans[curr_loans.columns[i]])

# Delete snapshots that are greater than the current month
curr_week_end = np.datetime64(curr_week_end)
final = curr_loans_2[curr_loans_2.snapshot <= curr_week_end]


# File will be split into two for further processing to speed up execution
final_accessed = final[final.ever_accessed == 'Yes']

# Sort
final_accessed.sort_values(['user_reference', 'snapshot'], inplace = True)

# reset index
final_accessed.reset_index(drop=True, inplace=True)

final_accessed.to_csv('accessed2.csv', index=True)


# add prev dlq bucket
final_accessed['prev_status'] = '0. New'

# add new loan flag
final_accessed['new_loan'] = 'No'

for ind in final_accessed.index:
    if ind == 0 and final_accessed['snapshot'][ind] == final_accessed['cohort_end'][ind]:
        final_accessed['prev_status'][ind] = '0. non existent'
        final_accessed['new_loan'][ind] = 'Yes'
    elif ind == 0 and final_accessed['snapshot'][ind] != final_accessed['cohort_end'][ind]:
        final_accessed['prev_status'][ind] = '1. Current'
        final_accessed['new_loan'][ind] = 'No'
    else:
        if final_accessed['account_identifier'][ind] == final_accessed['account_identifier'][ind-1]:
            final_accessed['prev_status'][ind] = final_accessed['snapshot_status'][ind-1]
        elif final_accessed['account_identifier'][ind] != final_accessed['account_identifier'][ind-1]:
            if final_accessed['snapshot'][ind] == final_accessed['cohort_end'][ind]:
                final_accessed['prev_status'][ind] = '0. non existent'
                final_accessed['new_loan'][ind] = 'Yes'
            elif final_accessed['snapshot'][ind] != final_accessed['cohort_end'][ind]:
                final_accessed['prev_status'][ind] = '1. Current'
                final_accessed['new_loan'][ind] = 'No'
            
# Add new default flag
final_accessed['new_default'] = '3. NA'

for ind in final_accessed.index:
    if final_accessed['snapshot_status'][ind] == '5. 91+':
        if final_accessed['prev_status'][ind] != '5. 91+':
            final_accessed['new_default'][ind] = '1. Yes'
        else:
            final_accessed['new_default'][ind] = '2. No'


final_not_accessed = final[final.ever_accessed != 'Yes']

# Sort
final_not_accessed.sort_values(['user_reference', 'snapshot'], inplace = True)

# reset index
final_not_accessed.reset_index(drop=True, inplace=True)

# add prev dlq bucket
final_accessed['prev_status'] = '0. non existent'

# add new loan flag
final_accessed['new_loan'] = '0. No'

final_not_accessed['prev_status'] = np.where(final_not_accessed['snapshot'] == final_not_accessed['cohort_end'], '0. non existent', '1. Current')

final_not_accessed['new_loan'] = np.where(final_not_accessed['snapshot'] == final_not_accessed['cohort_end'], '1. Yes', '0. No')

# Add new default flag
final_not_accessed['new_default'] = '3. NA'

# Remerge

final = pd.concat([final_accessed, final_not_accessed], axis=0)

# Sort
final.sort_values(['user_reference', 'snapshot'], inplace = True)

# reset index
final.reset_index(drop=True, inplace=True)

# output the results to a spreadsheet
# final.to_csv(csv_out_name, index=False)


# Upload data back to the cloud 
pgbq.to_gbq(final, out_table, project_id, if_exists='replace')