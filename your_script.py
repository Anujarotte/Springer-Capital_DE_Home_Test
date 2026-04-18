import pandas as pd
import numpy as np

print("🚀 Starting Pipeline...")

# ========================
# 1. LOAD DATA
# ========================
lead_logs = pd.read_csv("lead_log.csv")
paid_transactions = pd.read_csv("paid_transactions.csv")
referral_rewards = pd.read_csv("referral_rewards.csv")
user_logs = pd.read_csv("user_logs.csv")
user_referral_logs = pd.read_csv("user_referral_logs.csv")
user_referral_statuses = pd.read_csv("user_referral_statuses.csv")
user_referrals = pd.read_csv("user_referrals.csv")

print("✅ Data Loaded")


# ========================
# 2. DATA CLEANING
# ========================

# Convert datetime
user_referrals['referral_at'] = pd.to_datetime(user_referrals['referral_at'], errors='coerce')
paid_transactions['transaction_at'] = pd.to_datetime(paid_transactions['transaction_at'], errors='coerce')
user_logs['membership_expired_date'] = pd.to_datetime(user_logs['membership_expired_date'], errors='coerce')

# Drop duplicates
user_referrals.drop_duplicates(inplace=True)
user_logs.drop_duplicates(inplace=True)
user_referral_logs.drop_duplicates(inplace=True)

# Remove critical nulls
user_referrals.dropna(subset=['referral_id', 'referrer_id'], inplace=True)


# ========================
# 3. FIX COLUMN NAME CONFLICTS (VERY IMPORTANT)
# ========================
user_logs.rename(columns={"id": "user_log_id"}, inplace=True)
user_referral_statuses.rename(columns={"id": "status_id", "created_at": "status_created_at"}, inplace=True)
referral_rewards.rename(columns={"id": "reward_id", "created_at": "reward_created_at"}, inplace=True)
lead_logs.rename(columns={"id": "lead_log_id", "created_at": "lead_created_at"}, inplace=True)
user_referral_logs.rename(columns={"created_at": "reward_granted_at"}, inplace=True)


# ========================
# 4. JOIN TABLES
# ========================
df = user_referrals.merge(
    user_referral_logs,
    left_on="referral_id",
    right_on="user_referral_id",
    how="left"
)

df = df.merge(
    user_logs,
    left_on="referrer_id",
    right_on="user_id",
    how="left"
)

df = df.merge(
    user_referral_statuses,
    left_on="user_referral_status_id",
    right_on="status_id",
    how="left"
)

df = df.merge(
    referral_rewards,
    left_on="referral_reward_id",
    right_on="reward_id",
    how="left"
)

df = df.merge(
    paid_transactions,
    on="transaction_id",
    how="left"
)

df = df.merge(
    lead_logs,
    left_on="referee_id",
    right_on="lead_id",
    how="left"
)

df.drop_duplicates(inplace=True)

print("✅ Tables Joined Successfully")


# ========================
# 5. TRANSFORMATIONS
# ========================

# Referral Source Category
df['referral_source_category'] = np.where(
    df['referral_source'] == 'User Sign Up', 'Online',
    np.where(df['referral_source'] == 'Draft Transaction', 'Offline',
             df['source_category'])
)

# InitCap (DO NOT break PAID/NEW)
for col in df.select_dtypes(include='object').columns:
    if col not in ['homeclub', 'transaction_status', 'transaction_type']:
        df[col] = df[col].astype(str).str.title()


# ========================
# 6. BUSINESS LOGIC
# ========================
def check_valid(row):
    try:
        membership_valid = (
            pd.isnull(row['membership_expired_date']) or
            row['membership_expired_date'] > row['referral_at']
        )

        if (
            pd.notnull(row['reward_value']) and row['reward_value'] > 0 and
            row['description'] == "Berhasil" and
            pd.notnull(row['transaction_id']) and
            str(row['transaction_status']).upper() == "PAID" and
            str(row['transaction_type']).upper() == "NEW" and
            pd.notnull(row['transaction_at']) and
            row['transaction_at'] > row['referral_at'] and
            row['transaction_at'].month == row['referral_at'].month and
            membership_valid and
            row['is_deleted'] == False and
            row['is_reward_granted'] == True
        ):
            return True

        if (
            row['description'] in ["Menunggu", "Tidak Berhasil"] and
            pd.isnull(row['reward_value'])
        ):
            return True

        # INVALID CONDITIONS
        if pd.notnull(row['reward_value']) and row['reward_value'] > 0 and row['description'] != "Berhasil":
            return False

        if pd.notnull(row['reward_value']) and row['reward_value'] > 0 and pd.isnull(row['transaction_id']):
            return False

        if (
            pd.isnull(row['reward_value']) and
            pd.notnull(row['transaction_id']) and
            str(row['transaction_status']).upper() == "PAID"
        ):
            return False

        if row['description'] == "Berhasil" and (pd.isnull(row['reward_value']) or row['reward_value'] == 0):
            return False

        if pd.notnull(row['transaction_at']) and row['transaction_at'] < row['referral_at']:
            return False

        return False

    except:
        return False


df['is_business_logic_valid'] = df.apply(check_valid, axis=1)

print("✅ Business Logic Applied")


# ========================
# 7. FINAL OUTPUT
# ========================
final_df = df[[
    'referral_id',
    'referral_source',
    'referral_source_category',
    'referral_at',
    'referrer_id',
    'name',
    'phone_number',
    'homeclub',
    'referee_id',
    'referee_name',
    'referee_phone',
    'description',
    'reward_value',
    'transaction_id',
    'transaction_status',
    'transaction_at',
    'transaction_location',
    'transaction_type',
    'updated_at',
    'reward_granted_at',
    'is_business_logic_valid'
]]

# Rename columns
final_df.rename(columns={
    'name': 'referrer_name',
    'phone_number': 'referrer_phone_number',
    'homeclub': 'referrer_homeclub',
    'description': 'referral_status'
}, inplace=True)

# Remove NULL values
final_df.dropna(inplace=True)


# ========================
# 8. SAVE OUTPUT
# ========================
final_df.to_csv("final_report.csv", index=False)

print("🎯 Final Report Generated Successfully!")

print(f"📊 Final Row Count: {len(final_df)}")