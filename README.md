# PeaQL

**PeaQL** is a query language and toolkit for working with data, designed for flexibility and extensibility.

## Features

- Custom query language for structured data
- TypeScript-first development
- Modern build tooling (Vite, Vitest, ESLint, Prettier)
- Extensible parser and execution context
- Utilities for testing and validation

## Getting Started

### Installation

```sh
npm install peaql
```

### Usage

```typescript
import { createDatabase } from 'peaql';

const db = createDatabase({users: [...]});
const result = db.execute('SELECT * FROM users WHERE age > ?', 21)
console.log(result);

// Compile and re-use a statement
const statement = db.prepare('SELECT * FROM users WHERE age > :age')
console.log(db.execute(statement, {age: 23}));
```

## Scripts

- `npm run build` – Build the project with Vite
- `npm run lint` – Lint the source code
- `npm run check-types` – Type-check the codebase
- `npm test` – Run tests with Vitest
- `npm run fmt` – Format code with Prettier

## Development

- Source code is in `/src`
- Tests are in `/src/**/__tests__` or `/src/**/*.spec.ts`

## License

MIT

---

> **Note:** PeaQL is in early development.
