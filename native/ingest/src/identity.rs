use std::collections::HashMap;
#[derive(Default)]
pub struct Identity {
    ids: HashMap<String, usize>,
    parents: Vec<usize>,
    sizes: Vec<usize>,
    minima: Vec<usize>,
    names: Vec<String>,
}
impl Identity {
    fn insert(&mut self, name: &str) -> usize {
        if let Some(&i) = self.ids.get(name) {
            return i;
        }
        let i = self.parents.len();
        self.ids.insert(name.to_owned(), i);
        self.names.push(name.to_owned());
        self.parents.push(i);
        self.sizes.push(1);
        self.minima.push(i);
        i
    }
    fn find(&mut self, mut i: usize) -> usize {
        let mut root = i;
        while self.parents[root] != root {
            root = self.parents[root];
        }
        while i != root {
            let next = self.parents[i];
            self.parents[i] = root;
            i = next;
        }
        root
    }
    pub fn union(&mut self, first: Option<&str>, second: Option<&str>) {
        let (Some(a), Some(b)) = (
            first.filter(|s| !s.is_empty()),
            second.filter(|s| !s.is_empty()),
        ) else {
            return;
        };
        let a = self.insert(a);
        let b = self.insert(b);
        let (mut a, mut b) = (self.find(a), self.find(b));
        if a == b {
            return;
        }
        if self.sizes[a] < self.sizes[b] {
            std::mem::swap(&mut a, &mut b);
        }
        self.parents[b] = a;
        self.sizes[a] += self.sizes[b];
        if self.names[self.minima[b]].as_bytes() < self.names[self.minima[a]].as_bytes() {
            self.minima[a] = self.minima[b];
        }
    }
    pub fn freeze(mut self) -> HashMap<String, String> {
        let mut result = HashMap::with_capacity(self.names.len());
        for i in 0..self.names.len() {
            let root = self.find(i);
            result.insert(self.names[i].clone(), self.names[self.minima[root]].clone());
        }
        result
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn chains_cycles_unused_unicode_exact() {
        let mut ids = Identity::default();
        for i in (1..1000).rev() {
            ids.union(Some(&format!("user{i}")), Some(&format!("user{}", i - 1)));
        }
        for (a, b) in [
            ("user500", "Unused minimum"),
            ("device", "user999"),
            ("device", "other"),
            ("user0", "user999"),
            ("user0", "user0"),
            ("é", "中"),
            ("中", "z"),
            (" separate\r", "separate\t"),
        ] {
            ids.union(Some(a), Some(b));
        }
        ids.union(None, Some("AAA"));
        ids.union(Some(""), Some("AAA"));
        let frozen = ids.freeze();
        assert_eq!(frozen["user999"], "Unused minimum");
        assert_eq!(frozen["other"], "Unused minimum");
        assert_eq!(frozen["é"], "z");
        assert_eq!(frozen["separate\t"], " separate\r");
        assert!(!frozen.contains_key("AAA"));
        assert!(!frozen.contains_key("User0"));
    }
}
