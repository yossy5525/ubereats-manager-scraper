/**
 * UberEats Manager Cookie注入テスト (Phase 1)
 * 
 * 目的: Key-Value StoreからCookieを読み込み、ログイン状態を再現できるか確認
 */

import { Actor } from 'apify';
import { PlaywrightCrawler } from 'crawlee';

await Actor.init();

try {
    console.log('🚀 UberEats Manager Cookie注入テスト開始');
    
    // ========================================
    // STEP 1: Input取得
    // ========================================
    const input = await Actor.getInput() || {};
    const {
        cookieStoreId = 'nvTNFxnnM87yDL9jC',  // Store ID指定
        storeId = '9d065554-e3c2-5f05-9869-3e2666b78fa2',
        headless = true,
        debugMode = true,
    } = input;
    
    console.log('📥 Input:', { cookieStoreId, storeId, headless, debugMode });
    
    // ========================================
    // STEP 2: Cookie読み込み
    // ========================================
    console.log('🍪 Cookieを読み込み中...');
    const cookieStore = await Actor.openKeyValueStore(cookieStoreId);
    const cookies = await cookieStore.getValue('cookies');
    
    if (!cookies || !Array.isArray(cookies)) {
        throw new Error('❌ Cookieが見つかりません。Key-Value Storeに "cookies" を保存してください。');
    }
    
    console.log(`✅ ${cookies.length}個のCookieを読み込みました`);
    
    // Cookie有効期限チェック
    const now = Date.now() / 1000;
    let minDaysRemaining = Infinity;
    
    for (const cookie of cookies) {
        if (cookie.expirationDate && cookie.expirationDate > 0) {
            const daysRemaining = Math.floor((cookie.expirationDate - now) / 86400);
            if (daysRemaining < minDaysRemaining) {
                minDaysRemaining = daysRemaining;
            }
        }
    }
    
    if (minDaysRemaining <= 0) {
        throw new Error('🔴 Cookie期限切れ！手動で再取得してください。');
    } else if (minDaysRemaining <= 7) {
        console.log(`⚠️  Cookie残り${minDaysRemaining}日。まもなく期限切れです。`);
    } else {
        console.log(`✅ Cookie有効期限: あと${minDaysRemaining}日`);
    }
    
    // ========================================
    // STEP 3: Playwright Crawler設定
    // ========================================
    const crawler = new PlaywrightCrawler({
        launchContext: {
            launchOptions: {
                headless,
                args: [
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                ],
            },
        },
        
        // Cookie注入: ページ遷移前に実行
        preNavigationHooks: [
            async ({ page, request }) => {
                console.log(`🍪 Cookieをブラウザに注入中... (${request.url})`);
                
                // Playwright形式に変換
                const playwrightCookies = cookies.map(c => ({
                    name: c.name,
                    value: c.value,
                    domain: c.domain,
                    path: c.path || '/',
                    expires: c.expirationDate > 0 ? c.expirationDate : -1,
                    httpOnly: c.httpOnly || false,
                    secure: c.secure || false,
                    sameSite: c.sameSite === 'no_restriction' ? 'None' : 
                             c.sameSite === 'lax' ? 'Lax' : 
                             c.sameSite === 'strict' ? 'Strict' : 'Lax',
                }));
                
                await page.context().addCookies(playwrightCookies);
                console.log(`✅ ${playwrightCookies.length}個のCookieを注入しました`);
            },
        ],
        
        async requestHandler({ page, request, log }) {
            log.info(`🌐 ページにアクセス: ${request.url}`);
            
            // ランダム待機（人間らしく）
            const delay = Math.floor(Math.random() * 3000) + 2000;
            log.info(`⏳ ${delay}ms 待機中...`);
            await page.waitForTimeout(delay);
            
            // ========================================
            // STEP 4: ログイン状態確認
            // ========================================
            const currentUrl = page.url();
            log.info(`🔍 現在のURL: ${currentUrl}`);
            
            if (currentUrl.includes('/login') || currentUrl.includes('/signin')) {
                log.error('❌ ログインページにリダイレクトされました');
                log.error('→ Cookie が無効です。再取得が必要です。');
                
                if (debugMode) {
                    const screenshot = await page.screenshot({ fullPage: true });
                    await Actor.setValue('screenshot_login_failed', screenshot, { contentType: 'image/png' });
                }
                
                throw new Error('Cookie認証失敗');
            }
            
            // ページタイトル確認
            const title = await page.title();
            log.info(`📄 ページタイトル: ${title}`);
            
            // 「注文者グループの概要」テキストを探す
            const hasCustomerData = await page.evaluate(() => {
                return document.body.innerText.includes('注文者グループの概要') ||
                       document.body.innerText.includes('注文者分析データ') ||
                       document.body.innerText.includes('Customer');
            });
            
            if (hasCustomerData) {
                log.info('✅ ログイン成功！注文者分析データページが表示されています');
            } else {
                log.warning('⚠️  ログインはできましたが、期待したコンテンツが見つかりません');
            }
            
            // ========================================
            // STEP 5: デバッグ情報保存
            // ========================================
            if (debugMode) {
                // スクリーンショット保存
                log.info('📸 スクリーンショットを保存中...');
                const screenshot = await page.screenshot({ fullPage: true });
                await Actor.setValue('screenshot_success', screenshot, { contentType: 'image/png' });
                
                // HTML保存
                const html = await page.content();
                await Actor.setValue('page_html', html, { contentType: 'text/html' });
                
                log.info('✅ デバッグ情報を保存しました');
            }
            
            // ========================================
            // 完了
            // ========================================
            log.info('');
            log.info('✅ Cookie注入テスト完了！');
            log.info('');
            log.info(`📊 結果サマリー:`);
            log.info(`   ログイン状態: 成功`);
            log.info(`   ページタイトル: ${title}`);
            log.info(`   Cookie数: ${cookies.length}個`);
            log.info(`   Cookie有効期限: あと${minDaysRemaining}日`);
        },
        
        maxRequestsPerCrawl: 1,
        maxConcurrency: 1,
    });
    
    // ========================================
    // STEP 6: クロール実行
    // ========================================
    const targetUrl = `https://merchants.ubereats.com/manager/home/${storeId}/analytics/customers/?dateRangePreset=last_12_weeks`;
    console.log(`📄 対象URL: ${targetUrl}`);
    
    await crawler.run([targetUrl]);
    
    console.log('');
    console.log('🎉 すべて完了しました！');
    
} catch (error) {
    console.error('');
    console.error('❌ エラーが発生しました:');
    console.error(error.message);
    console.error('');
    throw error;
}

await Actor.exit();
