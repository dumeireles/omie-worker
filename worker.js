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

      console.log(`[Fila] Processando ID #${item.id}...`);

      await supabase
        .from('integration_queue')
        .update({ status: 'processing', updated_at: new Date() })
        .eq('id', item.id);

      // Pega app_key e app_secret enviados diretamente pelo Zapier
      const omiePayload = {
        call: item.payload.omie_call,
        app_key: item.payload.app_key,
        app_secret: item.payload.app_secret,
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
      const errorStr = JSON.stringify(errorMsg);
      console.error(`[Erro] Falha no ID #${item.id}:`, errorMsg);
      
      // Se for erro de consumo redundante, devolve para 'pending' para tentar depois
      if (errorStr.includes("REDUNDANT") || errorStr.includes("Consumo redundante")) {
        console.log(`[Omie] Redundância detectada. Reagendando ID #${item.id}...`);
        await supabase
          .from('integration_queue')
          .update({ status: 'pending', error_message: errorStr })
          .eq('id', item.id);
        
        // Aguarda 60 segundos antes da próxima tentativa
        await sleep(60000);
      } else {
        await supabase
          .from('integration_queue')
          .update({ status: 'failed', error_message: errorStr })
          .eq('id', item.id);
      }
    }

processQueue();
