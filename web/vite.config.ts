import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

const proxy = process.env.ZVUCAST_DEV_PROXY ?? 'http://127.0.0.1:8080'

export default defineConfig({
  plugins: [react()],
  server: { proxy: { '/api': proxy } },
})
