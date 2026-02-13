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
        debugMode = false,
    } = input || {};

    console.log('🚀 UberEats Manager CSVデータ収集開始');
    console.log(`📥 Input:`, JSON.stringify(input, null, 2));

    console.log('🍪 Cookieを読み込み中...');
    const cookieStore = await Actor.openKeyValueStore(cookieStoreId);
    const cookies = await cookieStore.getValue('cookies');

    if (!cookies || cookies.length === 0) {
        throw new Error('❌ Cookieが見つかりません');
    }
    console.log(`✅ ${cookies.length}個のCookieを読み込みました`);

    const sidCookie = cookies.find(c => c.name === 'sid');
    if (!sidCookie) {
        throw new Error('❌ 認証Cookie（sid）が見つかりません');
    }

    const now = Date.now() / 1000;
    if (sidCookie.expirationDate && sidCookie.expirationDate > 0) {
        const daysRemaining = Math.floor((sidCookie.expirationDate - now) / 86400);
        console.log(`✅ 認証Cookie（sid）有効期限: あと${daysRemaining}日`);
        
        if (daysRemaining <= 0) {
            throw new Error('🔴 Cookie期限切れ！至急更新してください。');
        }
        if (daysRemaining <= 7) {
            console.log(`⚠️ Cookie残り${daysRemaining}日。更新を準備してください。`);
        }
    }

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
            `${item.store_id}_${item.period_start}_${item.period_end}_${item.pincode}`
        )
    );
    console.log(`  - 位置情報データ: ${existingLocationKeys.size}件の既存レコード`);

    const targetUrl = `https://merchants.ubereats.com/manager/home/${storeId}/analytics/customers/?dateRangePreset=last_12_weeks`;
    console.log(`📄 対象URL: ${targetUrl}`);

    let newTrendsCount = 0;
    let newLocationsCount = 0;

    const crawler = new PlaywrightCrawler({
        headless,
        launchContext: {
            launchOptions: {
                acceptDownloads: true,
            },
        },
        preNavigationHooks: [
            async ({ page }, goToOptions) => {
                console.log('🍪 Cookieをブラウザに注入中...', goToOptions.url);
                
                // sameSite属性を正規化（Playwrightの要件: Strict | Lax | None のみ）
                const normalizedCookies = cookies.map(cookie => {
                    let sameSite = cookie.sameSite;
                    
                    // 不正な値を Lax に正規化
                    if (!sameSite || sameSite === 'unspecified' || sameSite === 'no_restriction') {
                        sameSite = 'Lax';
                    }
                    
                    // 大文字小文字を正規化
                    if (sameSite.toLowerCase() === 'strict') sameSite = 'Strict';
                    if (sameSite.toLowerCase() === 'lax') sameSite = 'Lax';
                    if (sameSite.toLowerCase() === 'none') sameSite = 'None';
                    
                    return {
                        ...cookie,
                        sameSite,
                    };
                });
                
                await page.context().addCookies(normalizedCookies);
                console.log(`✅ ${cookies.length}個のCookieを注入しました`);
            },
        ],
        requestHandler: async ({ page, request }) => {
            console.log(`🌐 ページにアクセス: ${request.url}`);
            await page.waitForTimeout(2000);

            const currentUrl = page.url();
            console.log(`🔍 現在のURL: ${currentUrl}`);

            const title = await page.title();
            console.log(`📄 ページタイトル: ${title}`);

            if (currentUrl.includes('login')) {
                throw new Error('❌ ログイン失敗：Cookieが無効です');
            }

            console.log('✅ ログイン成功！');
            console.log('');
            console.log('📍 位置情報CSVをダウンロード中...');
            
            // ページ下部へスクロール（位置情報セクションは最下部付近）
            await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
            await page.waitForTimeout(3000);
            
            // さらに少し上にスクロール（最下部だと見えない可能性があるため）
            await page.evaluate(() => window.scrollBy(0, -500));
            await page.waitForTimeout(2000);

            const locationDownloadPromise = page.waitForEvent('download');
            
            // すべてのダウンロードボタンを取得し、最初の1つをクリック（位置情報用）
            const downloadButtons = await page.locator('button:has-text("ダウンロード")').all();
            console.log(`   見つかったダウンロードボタン数: ${downloadButtons.length}`);
            
            if (downloadButtons.length === 0) {
                throw new Error('❌ ダウンロードボタンが見つかりません');
            }
            
            await downloadButtons[0].click();
            
            const locationDownload = await locationDownloadPromise;
            console.log(`✅ 位置情報CSVダウンロード開始: ${locationDownload.suggestedFilename()}`);

            const locationPath = await locationDownload.path();
            const locationCsvContent = readFileSync(locationPath, 'utf-8');
            console.log(`📄 位置情報CSV読み込み完了 (${locationCsvContent.length} bytes)`);

            const locationRecords = parse(locationCsvContent, {
                columns: true,
                skip_empty_lines: true,
                bom: true,
            });
            console.log(`📊 位置情報レコード数: ${locationRecords.length}`);

            const urlParams = new URL(request.url).searchParams;
            const periodPreset = urlParams.get('dateRangePreset') || 'last_12_weeks';
            const periodStart = urlParams.get('start') || '';
            const periodEnd = urlParams.get('end') || '';

            const newLocationRecords = [];
            for (const row of locationRecords) {
                const pincode = row['郵便番号'] || row['pincode'] || '';
                const key = `${storeId}_${periodStart}_${periodEnd}_${pincode}`;
                
                if (!existingLocationKeys.has(key)) {
                    newLocationRecords.push({
                        store_id: storeId,
                        store_name: storeName,
                        period_preset: periodPreset,
                        period_start: periodStart,
                        period_end: periodEnd,
                        pincode: pincode,
                        new_customers: parseInt(row['新着'] || row['new'] || 0),
                        occasional_customers: parseInt(row['低頻度'] || row['occasional'] || 0),
                        frequent_customers: parseInt(row['高頻度'] || row['frequent'] || 0),
                        total: parseInt(row['すべて'] || row['total'] || 0),
                        downloaded_at: new Date().toISOString(),
                    });
                }
            }

            if (newLocationRecords.length > 0) {
                await locationsDataset.pushData(newLocationRecords);
                console.log(`✅ 位置情報: ${newLocationRecords.length}件の新規レコードを保存`);
                newLocationsCount = newLocationRecords.length;
            } else {
                console.log('ℹ️  位置情報: 新規レコードなし');
            }

            console.log('');
            console.log('📈 注文者グループの傾向CSVをダウンロード中...');
            
            // 少し上にスクロール
            await page.evaluate(() => window.scrollBy(0, -300));
            await page.waitForTimeout(2000);

            const trendsDownloadPromise = page.waitForEvent('download');
            
            // 2つ目のダウンロードボタンをクリック（傾向用）
            const downloadButtons2 = await page.locator('button:has-text("ダウンロード")').all();
            console.log(`   見つかったダウンロードボタン数: ${downloadButtons2.length}`);
            
            if (downloadButtons2.length < 2) {
                throw new Error('❌ 傾向データのダウンロードボタンが見つかりません');
            }
            
            await downloadButtons2[1].click();
            
            const trendsDownload = await trendsDownloadPromise;
            console.log(`✅ 傾向CSVダウンロード開始: ${trendsDownload.suggestedFilename()}`);

            const trendsPath = await trendsDownload.path();
            const trendsCsvContent = readFileSync(trendsPath, 'utf-8');
            console.log(`📄 傾向CSV読み込み完了 (${trendsCsvContent.length} bytes)`);

            const trendsRecords = parse(trendsCsvContent, {
                columns: true,
                skip_empty_lines: true,
                bom: true,
            });
            console.log(`📊 傾向レコード数: ${trendsRecords.length}`);

            const newTrendsRecords = [];
            for (const row of trendsRecords) {
                const date = row['Date'] || row['date'] || '';
                const key = `${storeId}_${date}`;
                
                if (!existingTrendKeys.has(key)) {
                    const newVal = parseInt(row['New'] || row['new'] || 0);
                    const freqVal = parseInt(row['Frequent'] || row['frequent'] || 0);
                    const occVal = parseInt(row['Occasional'] || row['occasional'] || 0);
                    
                    newTrendsRecords.push({
                        store_id: storeId,
                        store_name: storeName,
                        date: date,
                        new_customers: newVal,
                        frequent_customers: freqVal,
                        occasional_customers: occVal,
                        total: newVal + freqVal + occVal,
                        downloaded_at: new Date().toISOString(),
                    });
                }
            }

            if (newTrendsRecords.length > 0) {
                await trendsDataset.pushData(newTrendsRecords);
                console.log(`✅ 傾向データ: ${newTrendsRecords.length}件の新規レコードを保存`);
                newTrendsCount = newTrendsRecords.length;
            } else {
                console.log('ℹ️  傾向データ: 新規レコードなし');
            }

            if (debugMode) {
                console.log('📸 デバッグ情報保存中...');
                await Actor.setValue('screenshot_phase2', await page.screenshot({ fullPage: true }), {
                    contentType: 'image/png',
                });
                
                await Actor.setValue('page_html_phase2', await page.content(), {
                    contentType: 'text/html',
                });
                console.log('✅ デバッグ情報を保存しました');
            }

            console.log('');
            console.log('✅ CSVダウンロード完了！');
            console.log('');
            console.log('📊 結果サマリー:');
            console.log(`   位置情報: ${newLocationsCount}件の新規レコード`);
            console.log(`   傾向データ: ${newTrendsCount}件の新規レコード`);
        },
        maxRequestsPerCrawl: 1,
    });

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
