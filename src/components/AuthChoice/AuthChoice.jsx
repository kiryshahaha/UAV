'use client'

import React, { useState } from 'react'
import styles from './AuthChoice.module.css'
import Login from '../login/Login'
import Password from '../password/Password'
import { supabase } from '@/lib/supabaseClient'

const AuthChoice = ({ onLoginSuccess }) => {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState('');

  const handleLogin = async () => {
    if (!email || !password) {
      setError('Введите email и пароль');
      return;
    }

    setIsLoading(true);
    setError('');

    try {
      const { data, error } = await supabase.auth.signInWithPassword({
        email: email.trim(),
        password: password,
      });

      if (error) {
        setError(error.message);
        return;
      }

      if (data.user) {
        // Получаем пользователя из auth.users
        const userInfo = {
          id: data.user.id,
          email: data.user.email,
          role: data.user.role || 'authenticated', // Используем нативную роль
          name: data.user.user_metadata?.name || data.user.email,
        };

        // Сохраняем в localStorage
        localStorage.setItem('user', JSON.stringify(userInfo));
        localStorage.setItem('supabase_token', data.session.access_token);
        
        if (onLoginSuccess && typeof onLoginSuccess === 'function') {
          onLoginSuccess(userInfo);
        }
      }
    } catch (error) {
      setError('Ошибка при входе');
      console.error('Login error:', error);
    } finally {
      setIsLoading(false);
    }
  };

  const handleKeyPress = (e) => {
    if (e.key === 'Enter') {
      handleLogin();
    }
  };

  return (
    <div className={styles.container}>
        <div className={styles.mainText}>
            <span className={styles.text}>Вход</span>
        </div>
        
        {error && (
          <div className={styles.error}>
            {error}
          </div>
        )}

        <div className={styles.login}>
          <Login 
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            onKeyPress={handleKeyPress}
          />
        </div>
        <div className={styles.password}>
          <Password 
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            onKeyPress={handleKeyPress}
          />
        </div>
        
        <button 
          className={styles.loginButton}
          onClick={handleLogin}
          disabled={isLoading}
        >
          {isLoading ? 'Вход...' : 'Войти'}
        </button>
    </div>
  )
}

export default AuthChoice