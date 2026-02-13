import { Actor } from 'apify';
import { PlaywrightCrawler } from 'crawlee';
import { parse } from 'csv-parse/sync';
import { readFileSync } from 'fs';

await Actor.init();

try {
    const input = await Actor.getInput();
    const {
        cookieStoreId = 'nvTNFxnnM87yDL9jC',
        storeId = '9d065554-e3c2-5f05-9869-3e2666b78fa2',
        storeName = 'BLA_NC HIROSHIMA',
        headless = true,
        debugMode = true,
    } = input || {};

    console.log('🚀 UberEats Manager CSVデータ収集開始');
    console.log(`📥 Input:`, JSON.stringify({ cookieStoreId, storeId, storeName, headless, debugMode }, null, 2));

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // Cookie読み込み＆有効期限チェック
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    console.log('🍪 Cookieを読み込み中...');
    const cookieStore = await Actor.openKeyValueStore(cookieStoreId);
    const cookies = await cookieStore.getValue('cookies');

    if (!cookies || cookies.length === 0) {
        throw new Error('❌ Cookieが見つかりません。Key-Value Store にCookieを保存してください。');
    }
    console.log(`✅ ${cookies.length}個のCookieを読み込みました`);

    const sidCookie = cookies.find(c => c.name === 'sid');
    if (!sidCookie) {
        throw new Error('❌ 認証Cookie（sid）が見つかりません。Cookieを再取得してください。');
    }

    const now = Date.now() / 1000;
    if (sidCookie.expirationDate && sidCookie.expirationDate > 0) {
        const daysRemaining = Math.floor((sidCookie.expirationDate - now) / 86400);
        console.log(`✅ 認証Cookie（sid）有効期限: あと${daysRemaining}日`);
        if (daysRemaining <= 0) {
            throw new Error('🔴 Cookie期限切れ！至急更新してください。');
        }
        if (daysRemaining <= 7) {
            console.log(`⚠️ 【警告】Cookie残り${daysRemaining}日。更新を準備してください。`);
        }
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // Dataset準備 & 既存データ読み込み（重複排除用）
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    console.log('📊 Datasetを準備中...');
    const trendsDataset = await Actor.openDataset('ubereats-customer-trends');
    const locationsDataset = await Actor.openDataset('ubereats-customer-locations');

    const existingTrends = await trendsDataset.getData();
    const existingTrendKeys = new Set(
        existingTrends.items.map(item => `${item.store_id}_${item.date}`)
    );
    console.log(`  - 傾向データ: ${existingTrendKeys.size}件の既存レコード`);

    const existingLocations = await locationsDataset.getData();
    const existingLocationKeys = new Set(
        existingLocations.items.map(item =>
            `${item.store_id}_${item.period_preset}_${item.pincode}`
        )
    );
    console.log(`  - 位置情報データ: ${existingLocationKeys.size}件の既存レコード`);

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // ブラウザ起動 & ページアクセス
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    const targetUrl = `https://merchants.ubereats.com/manager/home/${storeId}/analytics/customers/?dateRangePreset=last_12_weeks`;
    console.log(`📄 対象URL: ${targetUrl}`);

    let newTrendsCount = 0;
    let newLocationsCount = 0;
    const errors = [];

    // CSVデータをカラム名に基づいて振り分ける関数
    async function processCSV(records, columns, filename) {
        const columnsLower = columns.map(c => c.toLowerCase());
        const isTrends = columnsLower.some(c => c === 'date' || c === '日付');
        const isLocations = columnsLower.some(c => c === '郵便番号' || c === 'pincode' || c === 'postal code');

        if (isTrends) {
            console.log(`📈 傾向データとして処理: ${filename}`);
            const newRecords = [];
            for (const row of records) {
                const date = row['Date'] || row['日付'] || row['date'] || '';
                const key = `${storeId}_${date}`;
                if (!existingTrendKeys.has(key)) {
                    const newVal = parseInt(row['New'] || row['新着'] || row['new'] || 0);
                    const freqVal = parseInt(row['Frequent'] || row['高頻度'] || row['frequent'] || 0);
                    const occVal = parseInt(row['Occasional'] || row['低頻度'] || row['occasional'] || 0);
                    newRecords.push({
                        store_id: storeId,
                        store_name: storeName,
                        date,
                        new_customers: newVal,
                        frequent_customers: freqVal,
                        occasional_customers: occVal,
                        total: newVal + freqVal + occVal,
                        downloaded_at: new Date().toISOString(),
                    });
                    existingTrendKeys.add(key);
                }
            }
            if (newRecords.length > 0) {
                await trendsDataset.pushData(newRecords);
                newTrendsCount += newRecords.length;
                console.log(`✅ 傾向データ: ${newRecords.length}件の新規レコードを保存`);
            } else {
                console.log('ℹ️  傾向データ: 新規レコードなし');
            }
        } else if (isLocations) {
            console.log(`📍 位置情報データとして処理: ${filename}`);
            const newRecords = [];
            for (const row of records) {
                const pincode = row['郵便番号'] || row['pincode'] || row['Postal Code'] || '';
                const key = `${storeId}_last_12_weeks_${pincode}`;
                if (!existingLocationKeys.has(key)) {
                    newRecords.push({
                        store_id: storeId,
                        store_name: storeName,
                        period_preset: 'last_12_weeks',
                        pincode,
                        new_customers: parseInt(row['新着'] || row['New'] || row['new'] || 0),
                        occasional_customers: parseInt(row['低頻度'] || row['Occasional'] || row['occasional'] || 0),
                        frequent_customers: parseInt(row['高頻度'] || row['Frequent'] || row['frequent'] || 0),
                        total: parseInt(row['すべて'] || row['Total'] || row['total'] || row['All'] || 0),
                        downloaded_at: new Date().toISOString(),
                    });
                    existingLocationKeys.add(key);
                }
            }
            if (newRecords.length > 0) {
                await locationsDataset.pushData(newRecords);
                newLocationsCount += newRecords.length;
                console.log(`✅ 位置情報: ${newRecords.length}件の新規レコードを保存`);
            } else {
                console.log('ℹ️  位置情報: 新規レコードなし');
            }
        } else {
            console.log(`⚠️ 不明なCSV形式: ${filename}`);
            console.log(`   カラム: ${JSON.stringify(columns)}`);
            console.log(`   最初のレコード: ${JSON.stringify(records[0])}`);
            await Actor.setValue(`unknown_csv_${Date.now()}`, JSON.stringify(records, null, 2), {
                contentType: 'application/json',
            });
        }
    }

    const crawler = new PlaywrightCrawler({
        headless,
        launchContext: {
            launchOptions: {
                acceptDownloads: true,
            },
        },
        navigationTimeoutSecs: 60,
        requestHandlerTimeoutSecs: 300,
        preNavigationHooks: [
            async ({ page }) => {
                console.log('🍪 Cookieをブラウザに注入中...');
                const normalizedCookies = cookies
                    .filter(c => c.name && c.domain)
                    .map(cookie => {
                        let sameSite = cookie.sameSite;
                        if (!sameSite || sameSite === 'unspecified' || sameSite === 'no_restriction') {
                            sameSite = 'Lax';
                        }
                        if (typeof sameSite === 'string') {
                            const lower = sameSite.toLowerCase();
                            if (lower === 'strict') sameSite = 'Strict';
                            else if (lower === 'lax') sameSite = 'Lax';
                            else if (lower === 'none') sameSite = 'None';
                            else sameSite = 'Lax';
                        }
                        let domain = cookie.domain;
                        if (domain && domain.startsWith('.')) {
                            domain = domain.substring(1);
                        }
                        const result = {
                            name: cookie.name,
                            value: cookie.value || '',
                            domain,
                            path: cookie.path || '/',
                            secure: cookie.secure || false,
                            httpOnly: cookie.httpOnly || false,
                            sameSite,
                        };
                        if (cookie.expirationDate && cookie.expirationDate > 0) {
                            result.expires = cookie.expirationDate;
                        }
                        return result;
                    });
                await page.context().addCookies(normalizedCookies);
                console.log(`✅ ${normalizedCookies.length}個のCookieを注入しました`);
            },
        ],
        requestHandler: async ({ page, request }) => {
            console.log(`🌐 ページにアクセス: ${request.url}`);
            await page.waitForTimeout(5000);

            const currentUrl = page.url();
            console.log(`🔍 現在のURL: ${currentUrl}`);

            if (currentUrl.includes('login') || currentUrl.includes('auth')) {
                await Actor.setValue('screenshot_login_redirect', await page.screenshot({ fullPage: true }), {
                    contentType: 'image/png',
                });
                throw new Error('❌ ログイン失敗：Cookieが無効です。再取得してください。');
            }

            console.log(`📄 ページタイトル: ${await page.title()}`);
            console.log('✅ ログイン成功！');

            if (debugMode) {
                await Actor.setValue('screenshot_initial', await page.screenshot({ fullPage: true }), {
                    contentType: 'image/png',
                });
            }

            // ページコンテンツの読み込み待機
            console.log('⏳ ページコンテンツの読み込みを待機中...');
            try {
                await page.waitForSelector('button, a[download]', { timeout: 30000 });
            } catch (e) {
                console.log('⚠️ セレクタ待機タイムアウト。続行します...');
            }

            // ページ全体をスクロールしてコンテンツを読み込む
            await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
            await page.waitForTimeout(3000);
            await page.evaluate(() => window.scrollTo(0, 0));
            await page.waitForTimeout(2000);

            // デバッグ情報
            if (debugMode) {
                await Actor.setValue('screenshot_before_download', await page.screenshot({ fullPage: true }), {
                    contentType: 'image/png',
                });
                await Actor.setValue('page_html_full', await page.content(), {
                    contentType: 'text/html',
                });
                console.log('📸 ダウンロード前のスクリーンショット＆HTML保存');
            }

            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            // ダウンロードボタンを検出
            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            console.log('📥 ダウンロードボタンを検索中...');
            const downloadButtons = await page.locator('button:has-text("ダウンロード"), a[download], button:has-text("Download")').all();
            console.log(`📥 見つかったダウンロードボタン数: ${downloadButtons.length}`);

            if (downloadButtons.length === 0) {
                const allButtons = await page.locator('button').all();
                const buttonTexts = [];
                for (const btn of allButtons) {
                    const text = await btn.textContent().catch(() => '');
                    if (text.trim()) buttonTexts.push(text.trim());
                }
                console.log(`📋 ページ内の全ボタン: ${JSON.stringify(buttonTexts)}`);
                await Actor.setValue('screenshot_no_buttons', await page.screenshot({ fullPage: true }), {
                    contentType: 'image/png',
                });
                throw new Error(`❌ ダウンロードボタンが見つかりません。ボタン一覧: ${JSON.stringify(buttonTexts.slice(0, 20))}`);
            }

            // ボタンの詳細情報をログ
            for (let i = 0; i < downloadButtons.length; i++) {
                const text = await downloadButtons[i].textContent().catch(() => '');
                console.log(`   ボタン${i}: "${text.trim()}"`);
            }

            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            // CSV 1: 最初のダウンロードボタン
            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            console.log('');
            console.log('📥 1つ目のCSVをダウンロード中...');
            try {
                await downloadButtons[0].scrollIntoViewIfNeeded();
                await page.waitForTimeout(1000);
                const dl1Promise = page.waitForEvent('download', { timeout: 30000 });
                await downloadButtons[0].click();
                const dl1 = await dl1Promise;
                const fn1 = dl1.suggestedFilename();
                console.log(`✅ CSVダウンロード完了: ${fn1}`);
                const p1 = await dl1.path();
                const c1 = readFileSync(p1, 'utf-8');
                console.log(`📄 CSV読み込み完了 (${c1.length} bytes)`);
                const r1 = parse(c1, { columns: true, skip_empty_lines: true, bom: true });
                console.log(`📊 レコード数: ${r1.length}`);
                if (r1.length > 0) {
                    const cols = Object.keys(r1[0]);
                    console.log(`📋 カラム名: ${JSON.stringify(cols)}`);
                    await processCSV(r1, cols, fn1);
                }
            } catch (e) {
                console.error(`❌ 1つ目のCSVダウンロードに失敗: ${e.message}`);
                errors.push(`CSV1: ${e.message}`);
            }

            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            // CSV 2: 2つ目のダウンロードボタン
            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            if (downloadButtons.length >= 2) {
                console.log('');
                console.log('📥 2つ目のCSVをダウンロード中...');
                try {
                    await downloadButtons[1].scrollIntoViewIfNeeded();
                    await page.waitForTimeout(1000);
                    const dl2Promise = page.waitForEvent('download', { timeout: 30000 });
                    await downloadButtons[1].click();
                    const dl2 = await dl2Promise;
                    const fn2 = dl2.suggestedFilename();
                    console.log(`✅ CSVダウンロード完了: ${fn2}`);
                    const p2 = await dl2.path();
                    const c2 = readFileSync(p2, 'utf-8');
                    console.log(`📄 CSV読み込み完了 (${c2.length} bytes)`);
                    const r2 = parse(c2, { columns: true, skip_empty_lines: true, bom: true });
                    console.log(`📊 レコード数: ${r2.length}`);
                    if (r2.length > 0) {
                        const cols = Object.keys(r2[0]);
                        console.log(`📋 カラム名: ${JSON.stringify(cols)}`);
                        await processCSV(r2, cols, fn2);
                    }
                } catch (e) {
                    console.error(`❌ 2つ目のCSVダウンロードに失敗: ${e.message}`);
                    errors.push(`CSV2: ${e.message}`);
                }
            }

            if (debugMode) {
                await Actor.setValue('screenshot_final', await page.screenshot({ fullPage: true }), {
                    contentType: 'image/png',
                });
            }

            console.log('');
            console.log('📊 結果サマリー:');
            console.log(`   傾向データ: ${newTrendsCount}件の新規レコード`);
            console.log(`   位置情報: ${newLocationsCount}件の新規レコード`);
            if (errors.length > 0) {
                console.log(`   ⚠️ エラー: ${errors.join(', ')}`);
            }
        },
        maxRequestsPerCrawl: 1,
    });

    await crawler.run([targetUrl]);

    console.log('');
    console.log('🎉 すべて完了しました！');
    console.log(`📊 最終結果: 傾向=${newTrendsCount}件, 位置情報=${newLocationsCount}件`);

} catch (error) {
    console.error('');
    console.error('❌ 致命的なエラーが発生しました:');
    console.error(error.message);
    console.error(error.stack);
    throw error;
}

await Actor.exit();
