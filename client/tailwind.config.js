/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        telegram: {
          blue: '#0088cc',
          darkBlue: '#006ba6',
          lightBlue: '#54a9eb',
          bg: '#17212b',
          sidebar: '#0e1621',
          chat: '#0e1621',
          message: '#182533',
          messageOut: '#2b5278',
          hover: '#202b36',
          border: '#0e1621',
          text: '#f5f5f5',
          textSecondary: '#708499',
        }
      }
    },
  },
  plugins: [],
}
