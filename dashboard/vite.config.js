import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    proxy: {
      '/pricing-actions': 'http://127.0.0.1:8080',
      '/recommendations': 'http://127.0.0.1:8080',
      '/alerts': 'http://127.0.0.1:8080',
      '/model': 'http://127.0.0.1:8080',
      '/sku': 'http://127.0.0.1:8080',
      '/health': 'http://127.0.0.1:8080',
    },
  },
})
