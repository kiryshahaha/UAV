"use client";
import React, { useEffect } from 'react'
import styles from './Auth.module.css'
import { useRouter } from 'next/navigation'
import { supabase } from '@/lib/supabaseClient'

import Background from '@/components/background/Background'
import AuthComponent from '@/components/authComponent/AuthComponent'

export default function Auth() {
  const router = useRouter();

  // Проверяем, не авторизован ли уже пользователь
  useEffect(() => {
    const checkAuth = async () => {
      const { data: { session } } = await supabase.auth.getSession();
      if (session) {
        router.push('/');
      }
    };

    checkAuth();
  }, [router]);

  const handleLoginSuccess = (user) => {
    console.log('Login successful, redirecting to home page');
    router.push('/');
  };

  return (
    <div className={styles.container}>
        <div className={styles.bg}>
            <Background />
        </div>
        <div className={styles.AuthComponent}>
          <AuthComponent onLoginSuccess={handleLoginSuccess} />
        </div>
    </div>
  )
}