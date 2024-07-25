export default {
  async fetch(request, env, ctx) {
    if (!env.TRANSCRIPTION_JOBS) {
      return new Response(JSON.stringify({ error: 'TRANSCRIPTION_JOBS KV namespace is not bound to the worker' }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    const url = new URL(request.url);
    const audioUrl = url.searchParams.get('audioUrl');

    if (!audioUrl) {
      return new Response(JSON.stringify({ error: 'Audio URL is missing' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    try {
      const job = await env.TRANSCRIPTION_JOBS.get(audioUrl, { type: 'json' });

      if (!job) {
        return this.createJob(request, env, audioUrl);
      }

      if (job.status === 'complete') {
        return new Response(JSON.stringify({ status: 'complete', result: job.result }), {
          headers: { 'Content-Type': 'application/json' },
        });
      }

      return this.processNextChunks(job, env, request);
    } catch (error) {
      console.error('Error:', error);
      return new Response(JSON.stringify({ error: error.message }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      });
    }
  },

  async createJob(request, env, audioUrl) {
    try {
      const response = await fetch(audioUrl, { method: 'HEAD' });

      if (!response.ok) {
        return new Response(JSON.stringify({ error: 'Failed to fetch audio URL' }), {
          status: 400,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      const totalSize = parseInt(response.headers.get('Content-Length'), 10);
      const CHUNK_SIZE = 0.5 * 1024 * 1024; // 0.5MB chunks for better performance
      const chunks = Math.ceil(totalSize / CHUNK_SIZE);

      const job = {
        audioUrl,
        totalSize,
        chunkSize: CHUNK_SIZE,
        chunks,
        processedChunks: 0,
        result: '',
        status: 'processing',
      };

      await env.TRANSCRIPTION_JOBS.put(audioUrl, JSON.stringify(job));

      return new Response(JSON.stringify({ chunks }), { status: 201, headers: { 'Content-Type': 'application/json' } });
    } catch (error) {
      console.error('Error creating job:', error);
      return new Response(JSON.stringify({ error: 'Failed to create job' }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      });
    }
  },

  async processNextChunks(job, env, request) {
    const CHUNKS_TO_PROCESS = 5; // Number of chunks to process in parallel

    if (job.processedChunks >= job.chunks) {
      job.status = 'complete';
      await env.TRANSCRIPTION_JOBS.put(job.audioUrl, JSON.stringify(job));
      return new Response(JSON.stringify({ status: 'complete', result: job.result }), {
        headers: { 'Content-Type': 'application/json' },
      });
    }

    const processChunk = async (chunkIndex) => {
      const start = chunkIndex * job.chunkSize;
      const end = Math.min(start + job.chunkSize - 1, job.totalSize - 1);

      const response = await fetch(job.audioUrl, {
        headers: { Range: `bytes=${start}-${end}` },
      });

      const arrayBuffer = await response.arrayBuffer();
      const uint8Array = new Uint8Array(arrayBuffer);

      const inputs = {
        audio: Array.from(uint8Array),
      };

      const result = await env.AI.run('@cf/openai/whisper', inputs);
      return result.text;
    };

    const chunkPromises = [];
    for (let i = job.processedChunks; i < job.processedChunks + CHUNKS_TO_PROCESS && i < job.chunks; i++) {
      chunkPromises.push(processChunk(i));
    }

    const results = await Promise.all(chunkPromises);
    job.result += results.join(' ');
    job.processedChunks += results.length;

    if (job.processedChunks >= job.chunks) {
      job.status = 'complete';
    }

    await env.TRANSCRIPTION_JOBS.put(job.audioUrl, JSON.stringify(job));

    return new Response(JSON.stringify({ status: job.status, result: job.result, processedChunks: job.processedChunks, chunks: job.chunks }), { status: 200 });
  }
};
