use super::parse::{day_of, first, first_two, for_lines, count_lines, parse_price, parse_ts, split, B, DAY_MS};
use super::util::{hash, Map, Tab};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Barrier;


pub struct LinkMap<'a> {
    tab: Tab,
    ids: Vec<B<'a>>,
    hashes: Vec<u64>,
    name_of: Vec<u32>,
}

impl<'a> LinkMap<'a> {
    #[inline]
    pub fn name(&self, user: B<'a>, h: u64) -> (B<'a>, u64) {
        match self.tab.find(h, |i| self.ids[i as usize] == user) {
            Ok((_, i)) => {
                let n = self.name_of[i as usize] as usize;
                (self.ids[n], self.hashes[n])
            }
            Err(_) => (user, h),
        }
    }
}

fn find_seq(parent: &mut [u32], mut x: u32) -> u32 {
    let mut r = x;
    while parent[r as usize] != r {
        r = parent[r as usize];
    }
    while parent[x as usize] != r {
        let n = parent[x as usize];
        parent[x as usize] = r;
        x = n;
    }
    r
}

pub type LinkPart<'a> = Vec<(B<'a>, u64, B<'a>, u64)>;

pub fn parse_links(c: B<'_>) -> LinkPart<'_> {
    let mut v = Vec::with_capacity(count_lines(c) + 1);
    for_lines(c, |line| {
        if let Some([a, b]) = split::<2>(line) {
            if !a.is_empty() && !b.is_empty() {
                v.push((a, hash(a), b, hash(b)));
            }
        }
    });
    v
}

pub fn build_links_seq<'a>(parts: &[LinkPart<'a>]) -> LinkMap<'a> {
    let n: usize = parts.iter().map(|p| p.len()).sum();
    let mut tab = Tab::with_capacity(2 * n + 16);
    let mut ids: Vec<B<'a>> = Vec::with_capacity(n);
    let mut hashes: Vec<u64> = Vec::with_capacity(n);
    let mut parent: Vec<u32> = Vec::with_capacity(n);
    let mut intern = |s: B<'a>, h: u64, ids: &mut Vec<B<'a>>, hashes: &mut Vec<u64>, parent: &mut Vec<u32>| -> u32 {
        match tab.find(h, |i| ids[i as usize] == s) {
            Ok((_, i)) => i,
            Err(slot) => {
                let i = ids.len() as u32;
                ids.push(s);
                hashes.push(h);
                parent.push(i);
                tab.set(slot, h, i);
                i
            }
        }
    };
    for p in parts {
        for &(a, ha, b, hb) in p {
            let x = intern(a, ha, &mut ids, &mut hashes, &mut parent);
            let y = intern(b, hb, &mut ids, &mut hashes, &mut parent);
            let rx = find_seq(&mut parent, x);
            let ry = find_seq(&mut parent, y);
            if rx != ry {
                if rx < ry {
                    parent[ry as usize] = rx
                } else {
                    parent[rx as usize] = ry
                }
            }
        }
    }
    let m = ids.len();
    let mut min_of: Vec<u32> = (0..m as u32).collect();
    let mut roots = vec![0u32; m];
    for i in 0..m as u32 {
        let r = find_seq(&mut parent, i);
        roots[i as usize] = r;
        if ids[i as usize] < ids[min_of[r as usize] as usize] {
            min_of[r as usize] = i;
        }
    }
    let name_of: Vec<u32> = roots.iter().map(|&r| min_of[r as usize]).collect();
    LinkMap { tab, ids, hashes, name_of }
}

#[inline]
fn find_atomic(parent: &[AtomicU32], mut x: u32) -> u32 {
    let mut p = parent[x as usize].load(Ordering::Relaxed);
    while p != x {
        let gp = parent[p as usize].load(Ordering::Relaxed);
        let _ = parent[x as usize].compare_exchange_weak(p, gp, Ordering::Relaxed, Ordering::Relaxed);
        x = gp;
        p = parent[x as usize].load(Ordering::Relaxed);
    }
    x
}

#[inline]
fn union_atomic(parent: &[AtomicU32], mut x: u32, mut y: u32) {
    loop {
        let mut rx = find_atomic(parent, x);
        let mut ry = find_atomic(parent, y);
        if rx == ry {
            return;
        }
        if rx > ry {
            std::mem::swap(&mut rx, &mut ry);
        }
        match parent[ry as usize].compare_exchange(ry, rx, Ordering::Release, Ordering::Relaxed) {
            Ok(_) => return,
            Err(_) => {
                x = rx;
                y = parent[ry as usize].load(Ordering::Relaxed);
            }
        }
    }
}

pub fn build_links_par<'a>(parts: &[LinkPart<'a>], threads: usize) -> LinkMap<'a> {
    let total_edges: usize = parts.iter().map(|p| p.len()).sum();
    if total_edges == 0 {
        return LinkMap {
            tab: Tab::with_capacity(16),
            ids: Vec::new(),
            hashes: Vec::new(),
            name_of: Vec::new(),
        };
    }
    let threads = threads.max(1);
    if threads == 1 || total_edges < 32768 {
        return build_links_seq(parts);
    }

    const S: usize = 64;
    #[inline(always)]
    fn shard_idx(h: u64) -> usize {
        ((h >> 32) as usize) & (S - 1)
    }

    let chunk_size = (total_edges / (threads * 4)).clamp(128, 8192);
    let mut edge_chunks: Vec<&[(B<'a>, u64, B<'a>, u64)]> = Vec::new();
    for p in parts {
        for c in p.chunks(chunk_size) {
            if !c.is_empty() {
                edge_chunks.push(c);
            }
        }
    }

    let n_chunks = edge_chunks.len();
    let chunk_idx = AtomicUsize::new(0);

    struct ShardData<'a> {
        ids: Vec<B<'a>>,
        hashes: Vec<u64>,
        tab: Tab,
    }

    let thread_shards: Vec<Vec<Vec<(B<'a>, u64)>>> = std::thread::scope(|s| {
        let chunk_idx_ref = &chunk_idx;
        let edge_chunks_ref = &edge_chunks;
        let mut handles = Vec::with_capacity(threads);
        for _ in 0..threads {
            handles.push(s.spawn(move || {
                let mut local = (0..S).map(|_| Vec::new()).collect::<Vec<_>>();
                loop {
                    let idx = chunk_idx_ref.fetch_add(1, Ordering::Relaxed);
                    if idx >= n_chunks {
                        break;
                    }
                    let chunk = edge_chunks_ref[idx];
                    for &(a, ha, b, hb) in chunk {
                        local[shard_idx(ha)].push((a, ha));
                        local[shard_idx(hb)].push((b, hb));
                    }
                }
                local
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let shard_counter = AtomicUsize::new(0);
    let shard_outputs: Vec<Vec<(usize, ShardData<'a>)>> = std::thread::scope(|s| {
        let thread_shards_ref = &thread_shards;
        let shard_counter_ref = &shard_counter;
        let mut handles = Vec::with_capacity(threads);
        for _ in 0..threads {
            handles.push(s.spawn(move || {
                let mut my_shards = Vec::new();
                loop {
                    let s_idx = shard_counter_ref.fetch_add(1, Ordering::Relaxed);
                    if s_idx >= S {
                        break;
                    }
                    let total_items: usize = thread_shards_ref.iter().map(|t| t[s_idx].len()).sum();
                    let mut tab = Tab::with_capacity(total_items + 16);
                    let mut ids: Vec<B<'a>> = Vec::with_capacity(total_items / 2 + 8);
                    let mut hashes: Vec<u64> = Vec::with_capacity(ids.capacity());
                    for t in thread_shards_ref {
                        for &(item, h) in &t[s_idx] {
                            match tab.find(h, |idx| ids[idx as usize] == item) {
                                Ok(_) => {}
                                Err(slot) => {
                                    let idx = ids.len() as u32;
                                    ids.push(item);
                                    hashes.push(h);
                                    tab.set(slot, h, idx);
                                }
                            }
                        }
                    }
                    my_shards.push((s_idx, ShardData { ids, hashes, tab }));
                }
                my_shards
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let mut shard_results: Vec<Option<ShardData<'a>>> = (0..S).map(|_| None).collect();
    for thread_res in shard_outputs {
        for (s_idx, sdata) in thread_res {
            shard_results[s_idx] = Some(sdata);
        }
    }
    let shard_results: Vec<ShardData<'a>> = shard_results.into_iter().map(|o| o.unwrap()).collect();

    let mut shard_base = [0usize; S + 1];
    for s in 0..S {
        shard_base[s + 1] = shard_base[s] + shard_results[s].ids.len();
    }
    let total_unique = shard_base[S];
    if total_unique == 0 {
        return LinkMap {
            tab: Tab::with_capacity(16),
            ids: Vec::new(),
            hashes: Vec::new(),
            name_of: Vec::new(),
        };
    }

    let parent: Vec<AtomicU32> = (0..total_unique as u32).map(AtomicU32::new).collect();
    let mut ids: Vec<B<'a>> = Vec::with_capacity(total_unique);
    let mut hashes: Vec<u64> = Vec::with_capacity(total_unique);
    for s in 0..S {
        ids.extend_from_slice(&shard_results[s].ids);
        hashes.extend_from_slice(&shard_results[s].hashes);
    }
    let min_of: Vec<AtomicU32> = (0..total_unique as u32).map(AtomicU32::new).collect();
    let roots: Vec<AtomicU32> = (0..total_unique as u32).map(|_| AtomicU32::new(0)).collect();
    let name_of: Vec<AtomicU32> = (0..total_unique as u32).map(|_| AtomicU32::new(0)).collect();

    let item_chunk_size = (total_unique / (threads * 4)).clamp(64, 4096);
    let n_item_chunks = (total_unique + item_chunk_size - 1) / item_chunk_size;

    let chunk_idx2 = AtomicUsize::new(0);
    let item_chunk_counter = AtomicUsize::new(0);
    let name_chunk_counter = AtomicUsize::new(0);
    let barrier = Barrier::new(threads);

    std::thread::scope(|s| {
        let parent_ref = &parent;
        let shard_res_ref = &shard_results;
        let edge_chunks_ref = &edge_chunks;
        let chunk_idx2_ref = &chunk_idx2;
        let shard_base_ref = &shard_base;
        let min_of_ref = &min_of;
        let ids_ref = &ids;
        let roots_ref = &roots;
        let name_of_ref = &name_of;
        let item_chunk_counter_ref = &item_chunk_counter;
        let name_chunk_counter_ref = &name_chunk_counter;
        let barrier_ref = &barrier;

        for _ in 0..threads {
            s.spawn(move || {
                // Step A: Union edges
                loop {
                    let idx = chunk_idx2_ref.fetch_add(1, Ordering::Relaxed);
                    if idx >= n_chunks {
                        break;
                    }
                    let chunk = edge_chunks_ref[idx];
                    for &(a, ha, b, hb) in chunk {
                        let sa = shard_idx(ha);
                        let sb = shard_idx(hb);
                        let la = match shard_res_ref[sa].tab.find(ha, |i| shard_res_ref[sa].ids[i as usize] == a) {
                            Ok((_, i)) => i,
                            Err(_) => continue,
                        };
                        let lb = match shard_res_ref[sb].tab.find(hb, |i| shard_res_ref[sb].ids[i as usize] == b) {
                            Ok((_, i)) => i,
                            Err(_) => continue,
                        };
                        let x = (shard_base_ref[sa] as u32) + la;
                        let y = (shard_base_ref[sb] as u32) + lb;
                        if x != y {
                            union_atomic(parent_ref, x, y);
                        }
                    }
                }

                // Barrier 1: Wait for all edges to be unified
                barrier_ref.wait();

                // Step B: Find component roots & update min_of
                loop {
                    let c = item_chunk_counter_ref.fetch_add(1, Ordering::Relaxed);
                    if c >= n_item_chunks {
                        break;
                    }
                    let start = c * item_chunk_size;
                    let end = (start + item_chunk_size).min(total_unique);
                    for i in start..end {
                        let r = find_atomic(parent_ref, i as u32);
                        roots_ref[i].store(r, Ordering::Relaxed);
                        let r_idx = r as usize;
                        let mut curr = min_of_ref[r_idx].load(Ordering::Relaxed);
                        while ids_ref[i] < ids_ref[curr as usize] {
                            match min_of_ref[r_idx].compare_exchange_weak(curr, i as u32, Ordering::Release, Ordering::Relaxed) {
                                Ok(_) => break,
                                Err(actual) => curr = actual,
                            }
                        }
                    }
                }

                // Barrier 2: Wait for all minimums to be computed
                barrier_ref.wait();

                // Step C: Assign canonical name
                loop {
                    let c = name_chunk_counter_ref.fetch_add(1, Ordering::Relaxed);
                    if c >= n_item_chunks {
                        break;
                    }
                    let start = c * item_chunk_size;
                    let end = (start + item_chunk_size).min(total_unique);
                    for i in start..end {
                        let r = roots_ref[i].load(Ordering::Relaxed);
                        let best = min_of_ref[r as usize].load(Ordering::Relaxed);
                        name_of_ref[i].store(best, Ordering::Relaxed);
                    }
                }
            });
        }
    });

    let name_of: Vec<u32> = name_of.into_iter().map(AtomicU32::into_inner).collect();

    // Step 8: Build final global Tab
    let mut tab = Tab::with_capacity(2 * total_unique + 16);
    for (i, &h) in hashes.iter().enumerate() {
        let tag = h >> 32;
        let val = (tag << 32) | (i as u64 + 1);
        let mut slot = (h as usize) & tab.mask;
        loop {
            if tab.slots[slot] == 0 {
                tab.slots[slot] = val;
                break;
            }
            slot = (slot + 1) & tab.mask;
        }
    }

    LinkMap {
        tab,
        ids,
        hashes,
        name_of,
    }
}

pub fn build_links<'a>(parts: &[LinkPart<'a>], threads: usize) -> LinkMap<'a> {
    build_links_par(parts, threads)
}


pub const T_VIEW: u32 = 0;
pub const T_CART: u32 = 1;
pub const T_PURCHASE: u32 = 2;
pub const T_INVALID: u32 = 255;

#[derive(Clone, Copy)]
pub struct Ev<'a> {
    pub line: &'a u8,
    pub ingest: i64,
    pub ts: i64,
    pub pt: u32,
    pub cat: u32,
}
impl<'a> Ev<'a> {
    #[inline]
    fn eid(&self) -> B<'a> {
        unsafe { first(self.line) }
    }
    #[inline]
    fn user(&self) -> B<'a> {
        unsafe { first_two(self.line).1 }
    }
}

#[derive(Clone, Copy)]
pub struct Rec<'a> {
    pub name: B<'a>,
    pub nh: u64,
    pub ts: i64,
    pub pt: u32,
    pub cat: u32,
}

pub struct Parts<T> {
    pub items: Vec<T>,
    pub off: Vec<u32>,
}
impl<T> Default for Parts<T> {
    fn default() -> Self {
        Parts { items: Vec::new(), off: Vec::new() }
    }
}
impl<T: Copy> Parts<T> {
    fn group(tmp: &[(T, u32)], nshard: usize) -> Parts<T> {
        let mut off = vec![0u32; nshard + 1];
        for &(_, s) in tmp {
            off[s as usize + 1] += 1;
        }
        for i in 0..nshard {
            off[i + 1] += off[i];
        }
        let n = tmp.len();
        let mut items: Vec<T> = Vec::with_capacity(n);
        let mut pos: Vec<u32> = off[..nshard].to_vec();
        let p = items.as_mut_ptr();
        for &(x, s) in tmp {
            let q = &mut pos[s as usize];
            unsafe { p.add(*q as usize).write(x) };
            *q += 1;
        }
        unsafe { items.set_len(n) };
        Parts { items, off }
    }
    #[inline]
    pub fn shard(&self, s: usize) -> &[T] {
        if self.off.is_empty() {
            return &[];
        }
        &self.items[self.off[s] as usize..self.off[s + 1] as usize]
    }
}

pub struct EvCtx<'m, 'a> {
    pub ws: i64,
    pub we: i64,
    pub products: &'m Map<B<'a>, u32>,
    pub unknown_cat: u32,
}

#[inline]
pub fn shard_of(h: u64, nshard: usize) -> usize {
    ((h >> 32) as usize) & (nshard - 1)
}

pub fn parse_events<'a>(c: B<'a>, ctx: &EvCtx<'_, 'a>, nshard: usize, tmp: &mut Vec<(Ev<'a>, u32)>) -> Parts<Ev<'a>> {
    tmp.clear();
    for_lines(c, |line| {
        let Some([eid, user, ts, ingest, typ, product, price]) = split::<7>(line) else { return };
        if eid.is_empty() {
            return;
        }
        let Some(ingest) = parse_ts(ingest) else { return };
        let h = hash(eid);
        let mut e = Ev { line: &line[0], ingest, ts: 0, pt: T_INVALID << 24, cat: 0 };
        'v: {
            if user.is_empty() || product.is_empty() {
                break 'v;
            }
            let Some(t) = parse_ts(ts) else { break 'v };
            if t < ctx.ws || t >= ctx.we {
                break 'v;
            }
            let ty = match typ {
                b"view" => T_VIEW,
                b"cart" => T_CART,
                b"purchase" => T_PURCHASE,
                _ => break 'v,
            };
            let mut pr = 0u32;
            if ty == T_PURCHASE {
                let Some(p) = parse_price(price) else { break 'v };
                pr = p;
                e.cat = *ctx.products.get(product).unwrap_or(&ctx.unknown_cat);
            }
            if ingest - t > DAY_MS {
                break 'v;
            }
            e.ts = t;
            e.pt = pr | (ty << 24);
        }
        tmp.push((e, shard_of(h, nshard) as u32));
    });
    Parts::group(tmp, nshard)
}

#[derive(Default)]
pub struct DedupBufs<'a> {
    tab: Tab,
    base: Vec<u32>,
    win: Vec<bool>,
    tmp: Vec<(Rec<'a>, u32)>,
}

pub fn dedup_route<'a>(parts: &[Parts<Ev<'a>>], s: usize, links: &LinkMap<'a>, npshard: usize, b: &mut DedupBufs<'a>) -> Parts<Rec<'a>> {
    b.base.clear();
    let mut total = 0usize;
    for p in parts {
        b.base.push(total as u32);
        total += p.shard(s).len();
    }
    b.base.push(total as u32);
    let base = &b.base;
    let locate = |idx: u32| -> (usize, usize) {
        let c = base.partition_point(|&x| x <= idx) - 1;
        (c, (idx - base[c]) as usize)
    };
    b.tab.reset(total);
    b.win.clear();
    b.win.resize(total, true);
    for (ci, p) in parts.iter().enumerate() {
        let v = p.shard(s);
        for (i, e) in v.iter().enumerate() {
            let eid = e.eid();
            let eh = hash(eid);
            let flat = base[ci] + i as u32;
            let r = b.tab.find(eh, |idx| {
                let (c, j) = locate(idx);
                parts[c].shard(s)[j].eid() == eid
            });
            match r {
                Ok((slot, idx)) => {
                    let (c, j) = locate(idx);
                    if e.ingest < parts[c].shard(s)[j].ingest {
                        b.win[idx as usize] = false;
                        b.tab.set(slot, eh, flat);
                    } else {
                        b.win[flat as usize] = false;
                    }
                }
                Err(slot) => b.tab.set(slot, eh, flat),
            }
        }
    }
    b.tmp.clear();
    let mut k = 0usize;
    for p in parts {
        for e in p.shard(s) {
            let w = b.win[k];
            k += 1;
            if !w || (e.pt >> 24) > T_PURCHASE {
                continue;
            }
            let user = e.user();
            let (name, nh) = links.name(user, hash(user));
            b.tmp.push((Rec { name, nh, ts: e.ts, pt: e.pt, cat: e.cat }, shard_of(nh, npshard) as u32));
        }
    }
    Parts::group(&b.tmp, npshard)
}


#[derive(Clone, Copy)]
pub struct Ck0<'a> {
    pub line: &'a u8,
    pub ts: i64,
    pub camp: u32,
}

#[derive(Clone, Copy)]
pub struct Ck<'a> {
    pub name: B<'a>,
    pub nh: u64,
    pub line: &'a u8,
    pub ts: i64,
    pub camp: u32,
}

pub fn parse_clicks<'a>(c: B<'a>, ws: i64, we: i64, campaigns: &Map<B<'a>, u32>) -> Vec<Ck0<'a>> {
    let lo = ws - 7 * DAY_MS;
    let mut v = Vec::with_capacity(count_lines(c) + 1);
    for_lines(c, |line| {
        let Some([cid, user, camp, ts]) = split::<4>(line) else { return };
        if cid.is_empty() || user.is_empty() {
            return;
        }
        let Some(t) = parse_ts(ts) else { return };
        if t < lo || t >= we {
            return;
        }
        let Some(&ci) = campaigns.get(camp) else { return };
        v.push(Ck0 { line: &line[0], ts: t, camp: ci });
    });
    v
}

pub fn route_clicks<'a>(v: &[Ck0<'a>], links: &LinkMap<'a>, npshard: usize, tmp: &mut Vec<(Ck<'a>, u32)>) -> Parts<Ck<'a>> {
    tmp.clear();
    for c in v {
        let user = unsafe { first_two(c.line).1 };
        let (name, nh) = links.name(user, hash(user));
        tmp.push((Ck { name, nh, line: c.line, ts: c.ts, camp: c.camp }, shard_of(nh, npshard) as u32));
    }
    Parts::group(tmp, npshard)
}


pub const GAP_MS: i64 = 1_800_000;
pub const WEEK_MS: i64 = 7 * 86_400_000;

pub type Acc = Map<u64, [u64; 4]>;

#[inline]
pub fn key(camp: u32, day: i32) -> u64 {
    ((camp as u64) << 32) | (day as u32 as u64)
}
pub fn unkey(k: u64) -> (u32, i32) {
    ((k >> 32) as u32, k as u32 as i32)
}

#[derive(Clone, Copy)]
pub struct PE {
    pub ts: i64,
    pub pt: u32,
    pub cat: u32,
}

#[derive(Clone, Copy)]
pub struct PC<'a> {
    pub ts: i64,
    pub cid: B<'a>,
    pub camp: u32,
}

pub struct Sess {
    pub start: i64,
    pub day: i32,
    pub rev: u64,
    pub purch: bool,
}

pub struct UserRow<'a> {
    pub name: B<'a>,
    pub day: i32,
    pub sessions: u64,
    pub events: u64,
    pub revenue: u64,
    pub top: Option<u32>,
}

pub struct Scratch {
    sess: Vec<Sess>,
    starts: Vec<i64>,
    days: Vec<i32>,
    day_end: Vec<u32>,
    catsum: Vec<u64>,
    touched: Vec<u32>,
    ct: Vec<(u32, i64)>,
    rs: Vec<u32>,
}

impl Scratch {
    pub fn new(ncat: usize) -> Self {
        Scratch { sess: vec![], starts: vec![], days: vec![], day_end: vec![], catsum: vec![0; ncat], touched: vec![], ct: vec![], rs: vec![] }
    }
}

struct DayAcc {
    day: i32,
    sessions: u64,
    events: u64,
    revenue: u64,
}

fn flush_row<'a>(name: B<'a>, d: &DayAcc, sc: &mut Scratch, rows: &mut Vec<UserRow<'a>>) {
    let mut top: Option<u32> = None;
    let mut best = 0u64;
    for &c in &sc.touched {
        let v = sc.catsum[c as usize];
        if top.is_none() || v > best || (v == best && c < top.unwrap()) {
            top = Some(c);
            best = v;
        }
        sc.catsum[c as usize] = 0;
    }
    sc.touched.clear();
    rows.push(UserRow { name, day: d.day, sessions: d.sessions, events: d.events, revenue: d.revenue, top });
}

fn person<'a>(name: B<'a>, evs: &mut [PE], cks: &mut [PC<'a>], sc: &mut Scratch, rows: &mut Vec<UserRow<'a>>, organic: u32, acc: &mut Acc) {
    if evs.is_empty() {
        return;
    }
    evs.sort_unstable_by_key(|e| e.ts);
    sc.sess.clear();
    let mut d = DayAcc { day: day_of(evs[0].ts), sessions: 0, events: 0, revenue: 0 };
    let mut prev = evs[0].ts;
    for (i, e) in evs.iter().enumerate() {
        if i == 0 || e.ts - prev > GAP_MS {
            let day = day_of(e.ts);
            if day != d.day {
                flush_row(name, &d, sc, rows);
                d = DayAcc { day, sessions: 0, events: 0, revenue: 0 };
            }
            d.sessions += 1;
            sc.sess.push(Sess { start: e.ts, day, rev: 0, purch: false });
        }
        prev = e.ts;
        d.events += 1;
        if (e.pt >> 24) == T_PURCHASE {
            let price = (e.pt & 0x00ff_ffff) as u64;
            let s = sc.sess.last_mut().unwrap();
            s.rev += price;
            s.purch = true;
            d.revenue += price;
            let c = e.cat as usize;
            if sc.catsum[c] == 0 {
                sc.touched.push(e.cat);
            }
            sc.catsum[c] += price;
        }
    }
    flush_row(name, &d, sc, rows);

    if !cks.is_empty() {
        cks.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| a.cid.cmp(b.cid)));
    }
    credit(&sc.sess, cks, &mut sc.rs, organic, acc);
    if !cks.is_empty() {
        sc.starts.clear();
        sc.days.clear();
        sc.starts.extend(sc.sess.iter().map(|s| s.start));
        sc.days.extend(sc.sess.iter().map(|s| s.day));
        let n = sc.sess.len();
        sc.day_end.clear();
        sc.day_end.resize(n, 0);
        let mut end = n as u32;
        for i in (0..n).rev() {
            if i + 1 < n && sc.days[i + 1] != sc.days[i] {
                end = (i + 1) as u32;
            }
            sc.day_end[i] = end;
        }
        assists(&sc.starts, &sc.days, &sc.day_end, cks, &mut sc.ct, acc);
    }
}

#[derive(Default)]
pub struct ShardBufs<'a> {
    tab: Tab,
    names: Vec<B<'a>>,
    pid_ev: Vec<u32>,
    off: Vec<u32>,
    pos: Vec<u32>,
    pe: Vec<PE>,
    pid_ck: Vec<u32>,
    coff: Vec<u32>,
    pc: Vec<PC<'a>>,
}

pub fn run_shard<'a>(recs: &[Parts<Rec<'a>>], cks: &[Parts<Ck<'a>>], p: usize, organic: u32, sc: &mut Scratch, bf: &mut ShardBufs<'a>, acc: &mut Acc) -> Vec<UserRow<'a>> {
    let n_ev: usize = recs.iter().map(|r| r.shard(p).len()).sum();
    bf.tab.reset(n_ev);
    let names = &mut bf.names;
    names.clear();
    bf.pid_ev.clear();
    for r in recs {
        for e in r.shard(p) {
            let h = e.nh;
            let pid = match bf.tab.find(h, |i| names[i as usize] == e.name) {
                Ok((_, i)) => i,
                Err(slot) => {
                    let i = names.len() as u32;
                    names.push(e.name);
                    bf.tab.set(slot, h, i);
                    i
                }
            };
            bf.pid_ev.push(pid);
        }
    }
    let np = names.len();
    let off = &mut bf.off;
    off.clear();
    off.resize(np + 1, 0);
    for &pid in &bf.pid_ev {
        off[pid as usize + 1] += 1;
    }
    for i in 0..np {
        off[i + 1] += off[i];
    }
    bf.pos.clear();
    bf.pos.extend_from_slice(&off[..np]);
    bf.pe.clear();
    bf.pe.resize(n_ev, PE { ts: 0, pt: 0, cat: 0 });
    let mut k = 0usize;
    for r in recs {
        for e in r.shard(p) {
            let pid = bf.pid_ev[k] as usize;
            k += 1;
            bf.pe[bf.pos[pid] as usize] = PE { ts: e.ts, pt: e.pt, cat: e.cat };
            bf.pos[pid] += 1;
        }
    }
    bf.pid_ck.clear();
    let coff = &mut bf.coff;
    coff.clear();
    coff.resize(np + 1, 0);
    for c in cks {
        for x in c.shard(p) {
            let h = x.nh;
            let pid = match bf.tab.find(h, |i| names[i as usize] == x.name) {
                Ok((_, i)) => {
                    coff[i as usize + 1] += 1;
                    i
                }
                Err(_) => u32::MAX,
            };
            bf.pid_ck.push(pid);
        }
    }
    for i in 0..np {
        coff[i + 1] += coff[i];
    }
    bf.pos.clear();
    bf.pos.extend_from_slice(&coff[..np]);
    let nck_kept = coff[np] as usize;
    bf.pc.clear();
    bf.pc.resize(nck_kept, PC { ts: 0, cid: &[], camp: 0 });
    let mut k = 0usize;
    for c in cks {
        for x in c.shard(p) {
            let pid = bf.pid_ck[k];
            k += 1;
            if pid != u32::MAX {
                let q = &mut bf.pos[pid as usize];
                bf.pc[*q as usize] = PC { ts: x.ts, cid: unsafe { first(x.line) }, camp: x.camp };
                *q += 1;
            }
        }
    }
    let mut rows = Vec::with_capacity(np * 2);
    for i in 0..np {
        let evs = &mut bf.pe[off[i] as usize..off[i + 1] as usize];
        let cs = &mut bf.pc[coff[i] as usize..coff[i + 1] as usize];
        person(names[i], evs, cs, sc, &mut rows, organic, acc);
    }
    rows
}


fn credit(sess: &[Sess], cks: &[PC], rs: &mut Vec<u32>, organic: u32, acc: &mut Acc) {
    rs.clear();
    for i in 0..cks.len() {
        if i > 0 && cks[i].ts == cks[i - 1].ts {
            let r = rs[i - 1];
            rs.push(r);
        } else {
            rs.push(i as u32);
        }
    }
    let n = cks.len();
    let (mut lo, mut hi) = (0usize, 0usize);
    for s in sess {
        while hi < n && cks[hi].ts <= s.start {
            hi += 1;
        }
        while lo < hi && cks[lo].ts < s.start - WEEK_MS {
            lo += 1;
        }
        let camp = if hi > lo { cks[rs[hi - 1] as usize].camp } else { organic };
        let a = acc.entry(key(camp, s.day)).or_insert([0; 4]);
        a[0] += 1;
        a[1] += s.purch as u64;
        a[2] += s.rev;
    }
}

fn assists(starts: &[i64], days: &[i32], day_end: &[u32], cks: &[PC], ct: &mut Vec<(u32, i64)>, acc: &mut Acc) {
    ct.clear();
    ct.extend(cks.iter().map(|c| (c.camp, c.ts)));
    ct.sort_unstable();
    let n = ct.len();
    let mut flush = |camp: u32, a: i64, b: i64| {
        let lo = starts.partition_point(|&s| s < a);
        let hi = starts.partition_point(|&s| s <= b);
        let mut k = lo;
        while k < hi {
            let e = (day_end[k] as usize).min(hi);
            acc.entry(key(camp, days[k])).or_insert([0; 4])[3] += (e - k) as u64;
            k = e;
        }
    };
    let mut i = 0;
    while i < n {
        let camp = ct[i].0;
        let a = ct[i].1;
        let mut b = a + WEEK_MS;
        i += 1;
        while i < n && ct[i].0 == camp && ct[i].1 <= b {
            b = b.max(ct[i].1 + WEEK_MS);
            i += 1;
        }
        flush(camp, a, b);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_links_empty() {
        let lm = build_links(&[], 4);
        assert_eq!(lm.name(b"user1", hash(b"user1")), (b"user1".as_slice(), hash(b"user1")));
        let empty_parts: Vec<LinkPart> = vec![Vec::new(), Vec::new()];
        let lm2 = build_links(&empty_parts, 4);
        assert_eq!(lm2.name(b"user1", hash(b"user1")), (b"user1".as_slice(), hash(b"user1")));
    }

    #[test]
    fn test_build_links_basic_and_transitive() {
        let parts = vec![
            vec![
                (b"user_b".as_slice(), hash(b"user_b"), b"user_c".as_slice(), hash(b"user_c")),
                (b"user_c".as_slice(), hash(b"user_c"), b"user_a".as_slice(), hash(b"user_a")),
            ],
            vec![
                (b"user_d".as_slice(), hash(b"user_d"), b"user_e".as_slice(), hash(b"user_e")),
                (b"user_f".as_slice(), hash(b"user_f"), b"user_f".as_slice(), hash(b"user_f")), // self-link
            ],
        ];

        for threads in [1, 2, 4, 8] {
            let lm = build_links(&parts, threads);
            // user_a, user_b, user_c should all resolve to user_a (lexicographical smallest)
            assert_eq!(lm.name(b"user_a", hash(b"user_a")).0, b"user_a");
            assert_eq!(lm.name(b"user_b", hash(b"user_b")).0, b"user_a");
            assert_eq!(lm.name(b"user_c", hash(b"user_c")).0, b"user_a");

            // user_d, user_e should resolve to user_d
            assert_eq!(lm.name(b"user_d", hash(b"user_d")).0, b"user_d");
            assert_eq!(lm.name(b"user_e", hash(b"user_e")).0, b"user_d");

            // user_f is self-linked
            assert_eq!(lm.name(b"user_f", hash(b"user_f")).0, b"user_f");

            // unlinked user should resolve to itself
            assert_eq!(lm.name(b"user_z", hash(b"user_z")).0, b"user_z");
        }
    }

    #[test]
    fn test_build_links_arbitrary_utf8() {
        let parts = vec![vec![
            (b" \t spaced user ".as_slice(), hash(b" \t spaced user "), b"normal_user".as_slice(), hash(b"normal_user")),
            (b"\xf0\x9f\x98\x80 emoji".as_slice(), hash(b"\xf0\x9f\x98\x80 emoji"), b"\xe4\xbd\xa0\xe5\xa5\xbd".as_slice(), hash(b"\xe4\xbd\xa0\xe5\xa5\xbd")),
        ]];
        for threads in [1, 2, 4] {
            let lm = build_links(&parts, threads);
            // space (0x20) is smaller than 'n' (0x6e)
            assert_eq!(lm.name(b"normal_user", hash(b"normal_user")).0, b" \t spaced user ");
            assert_eq!(lm.name(b" \t spaced user ", hash(b" \t spaced user ")).0, b" \t spaced user ");

            // UTF-8 bytes comparison
            let smallest = if b"\xf0\x9f\x98\x80 emoji"[..] < b"\xe4\xbd\xa0\xe5\xa5\xbd"[..] {
                b"\xf0\x9f\x98\x80 emoji".as_slice()
            } else {
                b"\xe4\xbd\xa0\xe5\xa5\xbd".as_slice()
            };
            assert_eq!(lm.name(b"\xf0\x9f\x98\x80 emoji", hash(b"\xf0\x9f\x98\x80 emoji")).0, smallest);
            assert_eq!(lm.name(b"\xe4\xbd\xa0\xe5\xa5\xbd", hash(b"\xe4\xbd\xa0\xe5\xa5\xbd")).0, smallest);
        }
    }

    #[test]
    fn test_parallel_equivalence_large_graph() {
        let n_nodes = 5000;
        let node_names: Vec<String> = (0..n_nodes).map(|i| format!("id_{:06x}", (i * 31) % 99991)).collect();
        let mut parts: Vec<LinkPart> = vec![Vec::new(); 6];
        let mut rng = 987654321u64;

        // Generate 20,000 edges across 6 parts
        for i in 0..20_000 {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let u1 = (rng >> 32) as usize % n_nodes;
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let u2 = (rng >> 32) as usize % n_nodes;
            let s1 = node_names[u1].as_bytes();
            let s2 = node_names[u2].as_bytes();
            parts[i % 6].push((s1, hash(s1), s2, hash(s2)));
        }

        let seq_map = build_links_seq(&parts);
        for &t in &[2, 4, 8] {
            let par_map = build_links_par(&parts, t);
            for node in &node_names {
                let b = node.as_bytes();
                let h = hash(b);
                let (s_name, s_h) = seq_map.name(b, h);
                let (p_name, p_h) = par_map.name(b, h);
                assert_eq!(s_name, p_name, "Thread count {t} mismatch on node {node}");
                assert_eq!(s_h, p_h, "Thread count {t} hash mismatch on node {node}");
            }
        }
    }

    #[test]
    fn test_long_chain_adversarial() {
        let n = 20_000;
        let nodes: Vec<String> = (0..n).map(|i| format!("user_{:06}", i)).collect();
        let mut edges = Vec::new();
        for i in 0..n - 1 {
            let ab = nodes[i].as_bytes();
            let bb = nodes[i + 1].as_bytes();
            edges.push((ab, hash(ab), bb, hash(bb)));
        }
        let parts = vec![edges];
        let seq = build_links_seq(&parts);
        for t in [2, 4, 8] {
            let par = build_links_par(&parts, t);
            for i in [0, 1, 500, 1000, 9999, 19999] {
                let sb = nodes[i].as_bytes();
                let h = hash(sb);
                let (s_name, _) = seq.name(sb, h);
                let (p_name, _) = par.name(sb, h);
                assert_eq!(s_name, b"user_000000".as_slice());
                assert_eq!(p_name, b"user_000000".as_slice());
            }
        }
    }

    #[test]
    fn test_star_graph() {
        let n = 10_000;
        let hub = "hub_node";
        let nodes: Vec<String> = (0..n).map(|i| format!("leaf_{:06}", i)).collect();
        let mut edges = Vec::new();
        let hub_b = hub.as_bytes();
        let hub_h = hash(hub_b);
        for s in &nodes {
            let sb = s.as_bytes();
            edges.push((hub_b, hub_h, sb, hash(sb)));
        }
        let parts = vec![edges];
        let seq = build_links_seq(&parts);
        for t in [2, 4, 8] {
            let par = build_links_par(&parts, t);
            let (sh, _) = seq.name(hub_b, hub_h);
            let (ph, _) = par.name(hub_b, hub_h);
            assert_eq!(sh, ph);
            assert_eq!(sh, b"hub_node".as_slice());
            for s in &nodes[0..50] {
                let sb = s.as_bytes();
                let h = hash(sb);
                assert_eq!(seq.name(sb, h), par.name(sb, h));
            }
        }
    }

    #[test]
    fn test_utf8_complex_lexicographical() {
        let raw_ids: Vec<&[u8]> = vec![
            b" U1",
            b"\"U1\"",
            b"001",
            b"999",
            b"U1",
            b"u1",
            b"\x7f_del",
            b"\xc2\xa0_nbsp",
            b"\xe4\xbd\xa0_chinese",
            b"\xf0\x9f\x98\x80_emoji",
        ];

        let mut edges = Vec::new();
        for i in 0..raw_ids.len() - 1 {
            let a = raw_ids[i];
            let b = raw_ids[i + 1];
            edges.push((a, hash(a), b, hash(b)));
        }
        for i in (1..raw_ids.len()).rev() {
            let a = raw_ids[i];
            let b = raw_ids[i - 1];
            edges.push((a, hash(a), b, hash(b)));
        }
        for &a in &raw_ids {
            edges.push((a, hash(a), a, hash(a)));
        }

        let parts = vec![edges];
        let seq = build_links_seq(&parts);
        let min_expected = *raw_ids.iter().min().unwrap();

        for t in [1, 2, 4, 8] {
            let par = build_links_par(&parts, t);
            for &id in &raw_ids {
                let h = hash(id);
                let (s_name, _) = seq.name(id, h);
                let (p_name, _) = par.name(id, h);
                assert_eq!(s_name, min_expected, "Seq name for {:?} failed", id);
                assert_eq!(p_name, min_expected, "Par name for {:?} failed", id);
            }
        }
    }
}
