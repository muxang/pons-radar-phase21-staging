import { render } from 'preact';
import { App } from './app';
import './style.css';
import './upgrade.css';

render(<App />, document.getElementById('app')!);
