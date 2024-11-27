from database.client import SupabaseClient
from keys import KEYS


supabase_url = KEYS.Supabase.url
supabase_key = KEYS.Supabase.secret
supabase_client = SupabaseClient(url=supabase_url, key=supabase_key)
