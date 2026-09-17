import { defineConfig } from 'vite';
export default defineConfig({
  server: {
    port: 5173,
    proxy: Object.fromEntries([
      ['orders', 8000], ['payment', 8001], ['notification', 8002], ['analytics', 8003]
    ].map(([name, port]) => [`/api/${name}`, {
      target: `http://127.0.0.1:${port}`,
      rewrite: path => path.replace(`/api/${name}`, '') || '/'
    }]))
  }
});
