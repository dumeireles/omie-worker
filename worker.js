const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const OMIE_APP_KEY = process.env.OMIE_APP_KEY;
const OMIE_APP_SECRET = process.env.OMIE_APP_SECRET;

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
const DELAY_MS = 3500; // Tempo de espera de 3.5 segundos entre requests

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function processQueue() {
  console.log("Worker iniciado e escutando a fila...");
  
  while (true) {
    try {
      const { data: item, error } = await supabase
        .from('integration_queue')
        .select('*')
        .eq('status', 'pending')
        .order('created_at', { ascending: true })
        .limit(1)
        .maybeSingle();

      if (error) {
        console.error('Erro ao buscar Supabase:', error.message);
        await sleep(5000);
        continue;
      }

      if (!item) {
        await sleep(4000);
        continue;
      }

      console.log(`[Fila] Processando ID #${item.id}...`);

      await supabase
        .from('integration_queue')
        .update({ status: 'processing', updated_at: new Date() })
        .eq('id', item.id);

      const omiePayload = {
        call: item.payload.omie_call,
        app_key: OMIE_APP_KEY,
        app_secret: OMIE_APP_SECRET,
        param: item.payload.param
      };

      await axios.post(item.payload.omie_endpoint, omiePayload);

      await supabase
        .from('integration_queue')
        .update({ status: 'completed', updated_at: new Date() })
        .eq('id', item.id);

      console.log(`[Omie] Sucesso no ID #${item.id}!`);

    } catch (err) {
      const errorMsg = err.response?.data || err.message;
      console.error(`[Erro] Falha no ID:`, errorMsg);
      
      // Em caso de falha, marca como failed para não travar a fila
      try {
        await supabase
          .from('integration_queue')
          .update({ status: 'failed', error_message: JSON.stringify(errorMsg) })
          .eq('status', 'processing');
      } catch (e) {}
    }

    await sleep(DELAY_MS);
  }
}

processQueue();
