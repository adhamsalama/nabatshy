import { createContext, useContext } from 'react';

export const DemoModeContext = createContext(false);

export const useDemoMode = () => useContext(DemoModeContext);
