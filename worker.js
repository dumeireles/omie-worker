const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const axios = require('axios');

const app = express();
const PORT = process.env.PORT || 3000;
app.get('/', (req, res) => res.send('Worker Omie Ativo!'));
app.listen(PORT, () => console.log(`HTTP server rodando na porta ${PORT}`));

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

const DELAY_MS = 3500;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function processQueue() {
  console.log("Worker iniciado e escutando a fila...");
  
  while (true) {
    let currentItem = null;

    try {
      const { data: item, error } = await supabase
        .from('integration_queue')
        .select('*')
        .eq('status', 'pending')
        .order('created_at', { ascending: true })
        .limit(1)
        .maybeSingle();

      if (error || !item) {
        await sleep(4000);
        continue;
      }

      currentItem = item;
      console.log(`[Fila] Processando ID #${currentItem.id}...`);

      await supabase
        .from('integration_queue')
        .update({ status: 'processing', updated_at: new Date() })
        .eq('id', currentItem.id);

      const omiePayload = {
        call: currentItem.payload.omie_call,
        app_key: currentItem.payload.app_key,
        app_secret: currentItem.payload.app_secret,
        param: currentItem.payload.param
      };

      await axios.post(currentItem.payload.omie_endpoint, omiePayload);

      await supabase
        .from('integration_queue')
        .update({ status: 'completed', updated_at: new Date() })
        .eq('id', currentItem.id);

      console.log(`[Omie] Sucesso no ID #${currentItem.id}!`);

    } catch (err) {
      if (currentItem) {
        const errorMsg = err.response?.data || err.message;
        const errorStr = typeof errorMsg === 'object' ? JSON.stringify(errorMsg) : String(errorMsg);
        
        console.error(`[Erro] Falha no ID #${currentItem.id}:`, errorMsg);

        // Se o Omie bloquear por consulta redundante em menos de 60s
        if (errorStr.includes("REDUNDANT") || errorStr.includes("Consumo redundante")) {
          console.log(`[Omie] Redundância detectada. Reagendando ID #${currentItem.id}...`);
          
          await supabase
            .from('integration_queue')
            .update({ status: 'pending', error_message: errorStr, updated_at: new Date() })
            .eq('id', currentItem.id);

          // Aguarda 60s exigidos pelo Omie antes da próxima iteração
          await sleep(60000);
        } else {
          // Outros erros reais de validação ou payload incorreto
          await supabase
            .from('integration_queue')
            .update({ status: 'failed', error_message: errorStr, updated_at: new Date() })
            .eq('id', currentItem.id);
        }
      }
    }

    await sleep(DELAY_MS);
  }
}

processQueue();
